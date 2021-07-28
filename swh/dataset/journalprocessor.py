# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import concurrent.futures
from concurrent.futures import FIRST_EXCEPTION, ProcessPoolExecutor
import contextlib
from hashlib import sha1
import logging
import multiprocessing
from pathlib import Path
import time
from typing import Any, Dict, Mapping, Sequence, Tuple, Type

from confluent_kafka import TopicPartition
import tqdm

from swh.dataset.exporter import Exporter
from swh.dataset.utils import LevelDBSet
from swh.journal.client import JournalClient
from swh.journal.serializers import kafka_to_value
from swh.model.identifiers import origin_identifier
from swh.storage.fixer import fix_objects


class JournalClientOffsetRanges(JournalClient):
    """
    A subclass of JournalClient reading only inside some specific offset
    range. Partition assignments have to be manually given to the class.

    This client can only read a single topic at a time.
    """

    def __init__(
        self,
        *args,
        offset_ranges: Mapping[int, Tuple[int, int]] = None,
        assignment: Sequence[int] = None,
        progress_queue: multiprocessing.Queue = None,
        refresh_every: int = 200,
        **kwargs,
    ):
        """
        Args:
            offset_ranges: A mapping of partition_id -> (low, high) offsets
                that define the boundaries of the messages to consume.
            assignment: The list of partitions to assign to this client.
            progress_queue: a multiprocessing.Queue where the current
                progress will be reported.
            refresh_every: the refreshing rate of the progress reporting.
        """
        self.offset_ranges = offset_ranges
        self.progress_queue = progress_queue
        self.refresh_every = refresh_every
        self.assignment = assignment
        self.count = None
        self.topic_name = None
        kwargs["stop_on_eof"] = True  # Stop when the assignment is empty
        super().__init__(*args, **kwargs)

    def subscribe(self):
        self.topic_name = self.subscription[0]
        time.sleep(0.1)  # https://github.com/edenhill/librdkafka/issues/1983
        logging.debug("Changing assignment to %s", str(self.assignment))
        self.consumer.assign(
            [TopicPartition(self.topic_name, pid) for pid in self.assignment]
        )

    def process(self, *args, **kwargs):
        self.count = 0
        try:
            self.handle_committed_offsets()
            if self.assignment:
                super().process(*args, **kwargs)
        finally:
            self.progress_queue.put(None)

    def handle_committed_offsets(self,):
        """
        Handle already committed partition offsets before starting processing.
        """
        committed = self.consumer.committed(
            [TopicPartition(self.topic_name, pid) for pid in self.assignment]
        )
        for tp in committed:
            self.handle_offset(tp.partition, tp.offset)

    def handle_offset(self, partition_id, offset):
        """
        Check whether the client has reached the end of the current
        partition, and trigger a reassignment if that is the case.
        """
        if offset < 0:  # Uninitialized partition offset
            return

        if self.count % self.refresh_every == 0:
            self.progress_queue.put({partition_id: offset})

        if offset >= self.offset_ranges[partition_id][1] - 1:
            if partition_id in self.assignment:
                self.assignment = [
                    pid for pid in self.assignment if pid != partition_id
                ]
                self.subscribe()  # Actually, unsubscribes from the partition_id

    def deserialize_message(self, message):
        """
        Override of the message deserialization to hook the handling of the
        message offset.
        We also return the raw objects instead of deserializing them because we
        will need the partition ID later.
        """
        self.handle_offset(message.partition(), message.offset())
        self.count += 1
        return message


class ParallelJournalProcessor:
    """
    Reads the given object type from the journal in parallel.
    It creates one JournalExportWorker per process.
    """

    def __init__(
        self,
        config,
        exporters: Sequence[Tuple[Type[Exporter], Dict[str, Any]]],
        export_id: str,
        obj_type: str,
        node_sets_path: Path,
        processes: int = 1,
    ):
        """
        Args:
            config: the exporter config, which should also include the
                JournalClient configuration.
            exporters: a list of Exporter to process the objects
            export_id: a unique identifier for the export that will be used
                as part of a Kafka consumer group ID.
            obj_type: The type of SWH object to export.
            node_sets_path: A directory where to store the node sets.
            processes: The number of processes to run.
        """
        self.config = config
        self.exporters = exporters
        self.group_id = "swh-dataset-export-{}".format(export_id)
        self.obj_type = obj_type
        self.processes = processes
        self.node_sets_path = node_sets_path
        self.offsets = None

    def get_offsets(self):
        """
        First pass to fetch all the current low and high offsets of each
        partition to define the consumption boundaries.
        """
        if self.offsets is None:
            client = JournalClient(
                **self.config["journal"],
                object_types=[self.obj_type],
                group_id=self.group_id,
            )
            topic_name = client.subscription[0]
            topics = client.consumer.list_topics(topic_name).topics
            partitions = topics[topic_name].partitions

            self.offsets = {}

            def fetch_insert_partition_id(partition_id):
                tp = TopicPartition(topic_name, partition_id)
                (lo, hi) = client.consumer.get_watermark_offsets(tp)
                if lo != hi:
                    self.offsets[partition_id] = (lo, hi)

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.processes
            ) as executor:
                list(
                    tqdm.tqdm(
                        executor.map(fetch_insert_partition_id, partitions.keys()),
                        total=len(partitions),
                        desc="  - Partition offsets",
                    )
                )
        return self.offsets

    def run(self):
        """
        Run the parallel export.
        """
        offsets = self.get_offsets()
        to_assign = list(offsets.keys())

        manager = multiprocessing.Manager()
        q = manager.Queue()

        with ProcessPoolExecutor(self.processes + 1) as pool:
            futures = []
            for i in range(self.processes):
                futures.append(
                    pool.submit(
                        self.export_worker,
                        assignment=to_assign[i :: self.processes],
                        progress_queue=q,
                    )
                )
            futures.append(pool.submit(self.progress_worker, queue=q))

            concurrent.futures.wait(futures, return_when=FIRST_EXCEPTION)
            for f in futures:
                if f.running():
                    continue
                exc = f.exception()
                if exc:
                    pool.shutdown(wait=False)
                    f.result()
                    raise exc

    def progress_worker(self, queue=None):
        """
        An additional worker process that reports the current progress of the
        export between all the different parallel consumers and across all the
        partitions, by consuming the shared progress reporting Queue.
        """
        d = {}
        active_workers = self.processes
        offset_diff = sum((hi - lo) for lo, hi in self.offsets.values())
        desc = f"  - Journal export ({self.obj_type})"
        with tqdm.tqdm(total=offset_diff, desc=desc) as pbar:
            while active_workers:
                item = queue.get()
                if item is None:
                    active_workers -= 1
                    continue
                d.update(item)
                progress = sum(n - self.offsets[p][0] for p, n in d.items())
                pbar.set_postfix(
                    active_workers=active_workers, total_workers=self.processes
                )
                pbar.update(progress - pbar.n)

    def export_worker(self, assignment, progress_queue):
        worker = JournalProcessorWorker(
            self.config,
            self.exporters,
            self.group_id,
            self.obj_type,
            self.offsets,
            assignment,
            progress_queue,
            self.node_sets_path,
        )
        with worker:
            worker.run()


class JournalProcessorWorker:
    """
    Worker process that processes all the messages and calls the given exporters
    for each object read from the journal.
    """

    def __init__(
        self,
        config,
        exporters: Sequence[Tuple[Type[Exporter], Dict[str, Any]]],
        group_id: str,
        obj_type: str,
        offsets: Dict[int, Tuple[int, int]],
        assignment: Sequence[int],
        progress_queue: multiprocessing.Queue,
        node_sets_path: Path,
    ):
        self.config = config
        self.group_id = group_id
        self.obj_type = obj_type
        self.offsets = offsets
        self.assignment = assignment
        self.progress_queue = progress_queue

        self.node_sets_path = node_sets_path
        self.node_sets_path.mkdir(exist_ok=True, parents=True)
        self.node_sets: Dict[Tuple[int, str], LevelDBSet] = {}

        self.exporters = [
            exporter_class(config, **kwargs) for exporter_class, kwargs in exporters
        ]
        self.exit_stack: contextlib.ExitStack = contextlib.ExitStack()

    def __enter__(self):
        self.exit_stack.__enter__()
        for exporter in self.exporters:
            self.exit_stack.enter_context(exporter)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit_stack.__exit__(exc_type, exc_value, traceback)

    def get_node_set_for_object(self, partition_id: int, object_id: bytes):
        """
        Return an on-disk set object, which stores the nodes that have
        already been processed.

        Node sets are sharded by partition ID (as each object is guaranteed to
        be assigned to a deterministic Kafka partition) then by object ID
        prefix. The sharding path of each file looks like:

            .node_sets/{origin..content}/part-{0..256}/nodes-{0..f}.sqlite
        """
        # obj_id_prefix = "{:x}".format(object_id[0] % 16)
        obj_id_prefix = "all"  # disable sharding for now
        shard_id = (partition_id, obj_id_prefix)
        if shard_id not in self.node_sets:
            node_set_dir = (
                self.node_sets_path
                / self.obj_type
                / ("part-{}".format(str(partition_id)))
            )
            node_set_dir.mkdir(exist_ok=True, parents=True)
            node_set_file = node_set_dir / "nodes-{}.db".format(obj_id_prefix)
            node_set = LevelDBSet(node_set_file)
            self.exit_stack.enter_context(node_set)
            self.node_sets[shard_id] = node_set
        return self.node_sets[shard_id]

    def run(self):
        """
        Start a Journal client on the given assignment and process all the
        incoming messages.
        """
        client = JournalClientOffsetRanges(
            **self.config["journal"],
            object_types=[self.obj_type],
            group_id=self.group_id,
            debug="cgrp,broker",
            offset_ranges=self.offsets,
            assignment=self.assignment,
            progress_queue=self.progress_queue,
            **{"message.max.bytes": str(500 * 1024 * 1024)},
        )
        client.process(self.process_messages)

    def process_messages(self, messages):
        """
        Process the incoming Kafka messages.
        """
        for object_type, message_list in messages.items():
            fixed_objects_by_partition = collections.defaultdict(list)
            for message in message_list:
                fixed_objects_by_partition[message.partition()].extend(
                    fix_objects(object_type, [kafka_to_value(message.value())])
                )
            for partition, objects in fixed_objects_by_partition.items():
                for obj in objects:
                    self.process_message(object_type, partition, obj)

    def process_message(self, object_type, partition, obj):
        """
        Process a single incoming Kafka message if the object it refers to has
        not been processed yet.

        It uses an on-disk set to make sure that each object is only ever
        processed once.
        """
        if object_type == "origin_visit":
            origin_id = origin_identifier({"url": obj["origin"]})
            visit = obj["visit"]
            node_id = sha1(f"{origin_id}:{visit}".encode()).digest()
        elif object_type == "origin_visit_status":
            if obj["status"] not in ("partial", "full"):
                # Temporary visit object, not useful for the exports
                return
            origin_id = origin_identifier({"url": obj["origin"]})
            visit = obj["visit"]
            ts = obj["date"].timestamp()
            node_id = sha1(f"{origin_id}:{visit}:{ts}".encode()).digest()
        elif object_type == "origin":
            node_id = sha1(obj["url"].encode()).digest()
        elif object_type in ("content", "skipped_content"):
            node_id = obj["sha1_git"]
        else:
            node_id = obj["id"]

        node_set = self.get_node_set_for_object(partition, node_id)
        if not node_set.add(node_id):
            # Node already processed, skipping.
            return

        for exporter in self.exporters:
            try:
                exporter.process_object(object_type, obj)
            except Exception:
                logging.exception(
                    "Exporter %s: error while exporting the object: %s",
                    exporter.__class__.__name__,
                    str(obj),
                )
