# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import concurrent.futures
from concurrent.futures import FIRST_EXCEPTION, ProcessPoolExecutor
import contextlib
import json
import logging
import multiprocessing
from pathlib import Path
import time
from typing import (
    Any,
    Container,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
)

from confluent_kafka import Message, TopicPartition
import tqdm

from swh.dataset.exporter import Exporter
from swh.dataset.utils import LevelDBSet
from swh.journal.client import JournalClient
from swh.journal.serializers import kafka_to_value
from swh.storage.fixer import fix_objects

logger = logging.getLogger(__name__)


class JournalClientOffsetRanges(JournalClient):
    """
    A subclass of JournalClient reading only inside some specific offset
    range. Partition assignments have to be manually given to the class.

    This client can only read a single topic at a time.
    """

    def __init__(
        self,
        *args,
        offset_ranges: Mapping[int, Tuple[int, int]],
        assignment: Sequence[int],
        progress_queue: multiprocessing.Queue,
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
        self._messages_to_commit: List[Message] = []
        self._partitions_to_unsubscribe: Set[int] = set()
        self.count = None
        self.topic_name: Optional[str] = None
        kwargs["stop_on_eof"] = True  # Stop when the assignment is empty
        super().__init__(*args, **kwargs)

    def subscribe(self):
        self.topic_name = self.subscription[0]
        time.sleep(0.1)  # https://github.com/edenhill/librdkafka/issues/1983
        logger.debug("Changing assignment to %s", str(self.assignment))
        self.consumer.assign(
            [
                TopicPartition(
                    self.topic_name,
                    pid,
                    self.offset_ranges[pid][0],
                )
                for pid in self.assignment
            ]
        )

    def unsubscribe(self, partitions: Container[int]):
        assert self.assignment is not None
        self.assignment = [pid for pid in self.assignment if pid not in partitions]
        logger.debug("Changing assignment to %s", str(self.assignment))
        self.consumer.assign(
            [TopicPartition(self.topic_name, pid) for pid in self.assignment]
        )

    def process(self, worker_fn):
        self.count = 0
        try:
            if self.assignment:
                super().process(worker_fn)
        finally:
            self.progress_queue.put(None)

    def handle_offset(self, message):
        """
        Check whether the client has reached the end of the current
        partition, and trigger a reassignment if that is the case.
        """
        offset = message.offset()
        partition_id = message.partition()

        if offset < 0:  # Uninitialized partition offset
            return

        if self.count % self.refresh_every == 0:
            self.progress_queue.put({partition_id: offset})

        if offset >= self.offset_ranges[partition_id][1] - 1:
            if partition_id in self.assignment:
                self.progress_queue.put({partition_id: offset})
                # unsubscribe from partition but make sure current message's
                # offset will be committed after executing the worker_fn in
                # process(); see handle_messages() below
                self._messages_to_commit.append(message)
                # delay the unsubcription to handle_messages() to prevent
                # rdkakfa errors like
                #
                #   rd_kafka_assignment_partition_stopped:
                #       Assertion `rktp->rktp_started' failed
                #
                # in case the unsubscription from parition_id do actually tries
                # to subscribe an already depleted partition.
                self._partitions_to_unsubscribe.add(partition_id)

    def deserialize_message(self, message, object_type=None):
        """
        Override of the message deserialization to hook the handling of the
        message offset.
        We also return the raw objects instead of deserializing them because we
        will need the partition ID later.
        """
        self.handle_offset(message)
        self.count += 1
        return message

    def handle_messages(self, messages, worker_fn):
        """Override of the handle_messages() method to get a chance to commit messages.

        Make sure messages properly handled by `worker_fn` (executed in
        super()) do get committed in kafka even if their originating partition
        has been desubscribed from.

        This helps having a consistent view of the consumption of each
        partition at the end of the export process (EOF).

        """
        nb_processed, at_eof = super().handle_messages(messages, worker_fn)
        for msg in self._messages_to_commit:
            self.consumer.commit(message=msg)
        self._messages_to_commit.clear()
        if self._partitions_to_unsubscribe:
            partitions = list(self._partitions_to_unsubscribe)
            self._partitions_to_unsubscribe.clear()
            self.unsubscribe(partitions)

        return nb_processed, at_eof


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
        offset_margin: Optional[float] = None,
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
        prefix = self.config["journal"].get("group_id", "swh-dataset-export-")
        self.group_id = f"{prefix}{export_id}"
        self.obj_type = obj_type
        self.processes = processes
        self.node_sets_path = node_sets_path
        self.offsets = None
        self.offset_margin = offset_margin

    def get_offsets(self):
        """
        Compute (lo, high) offset boundaries for all partitions.

        First pass to fetch all the current low and high watermark offsets of each
        partition to define the consumption boundaries.

        If available, use committed offsets as lo offset boundaries.
        """
        if self.offsets is None:
            cfg = self.config["journal"].copy()
            cfg["object_types"] = [self.obj_type]
            cfg["group_id"] = self.group_id
            client = JournalClient(**cfg)
            topic_name = client.subscription[0]
            topics = client.consumer.list_topics(topic_name).topics
            partitions = topics[topic_name].partitions

            self.offsets = {}

            # LOW watermark offset: The offset of the earliest message in the
            #   topic/partition. If no messages have been written to the topic,
            #   the low watermark offset is set to 0. The low watermark will also
            #   be 0 if one message has been written to the partition (with
            #   offset 0).
            # HIGH watermark offset: the offset of the latest message in the
            #   topic/partition available for consumption + 1

            def fetch_insert_partition_id(partition_id):
                logger.debug("Fetching offset for partition %s", partition_id)
                tp = TopicPartition(topic_name, partition_id)
                (lo, hi) = client.consumer.get_watermark_offsets(tp)
                logger.debug(
                    "[%s] watermark offset (lo,hi)=(%s, %s)", partition_id, lo, hi
                )
                if hi > lo:
                    # hi == low means there is nothing in the partition to consume.
                    # If the partition is not empty, retrieve the committed offset,
                    # if any, to use it at lo offset.
                    logger.debug(
                        "Fetching committed offset for partition %s", partition_id
                    )
                    committed = client.consumer.committed([tp])[0]
                    logger.debug(
                        "[%s] committed offset: %s", partition_id, committed.offset
                    )
                    lo = max(lo, committed.offset)
                    if self.offset_margin:
                        # Using min() in case of precision loss when self.offset_margin
                        # is close to 1.0 and lo is very large
                        newlo = min(lo, int(self.offset_margin * lo))
                        logger.debug(
                            "Apply offset margin: reducing lo from %s to %s", lo, newlo
                        )
                        lo = newlo

                if hi > lo:
                    # do only process the partition is there are actually new
                    # messages to process (partition not empty and committed
                    # offset is behind the high watermark).
                    self.offsets[partition_id] = (lo, hi)

            logger.debug(
                "Fetching partition offsets using %s processes", self.processes
            )
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.processes
            ) as executor:
                list(
                    tqdm.tqdm(
                        executor.map(fetch_insert_partition_id, partitions.keys()),
                        total=len(partitions),
                        desc="  - Offset",
                    )
                )
            client.close()
        return self.offsets

    def run(self):
        """
        Run the parallel export.
        """
        offsets = self.get_offsets()
        to_assign = list(offsets.keys())
        if not to_assign:
            print(f"  - Export ({self.obj_type}): skipped (nothing to export)")
            return
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

            # Run processes until they all complete, or an error occurs
            concurrent.futures.wait(futures, return_when=FIRST_EXCEPTION)

            # Propagate potential exceptions
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
        desc = "  - Export"
        with tqdm.tqdm(total=offset_diff, desc=desc, unit_scale=True) as pbar:
            while active_workers:
                item = queue.get()
                if item is None:
                    active_workers -= 1
                    continue
                d.update(item)
                progress = sum(n + 1 - self.offsets[p][0] for p, n in d.items())
                pbar.set_postfix(
                    workers=f"{active_workers}/{self.processes}",
                )
                pbar.update(progress - pbar.n)

        # Write final consumer offsets to a save file
        (
            self.node_sets_path
            / self.obj_type
            / f"offsets-final-{int(time.time())}.json"
        ).write_text(json.dumps(d))

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
        suffix. The sharding path of each file looks like:

            .node_sets/{origin..content}/part-{0..256}/nodes-{0..f}.db
        """
        obj_id_suffix = "{:x}".format(object_id[-1] % 16)
        # obj_id_suffix = "all"  # uncomment to disable sharding
        shard_id = (partition_id, obj_id_suffix)
        if shard_id not in self.node_sets:
            node_set_dir = (
                self.node_sets_path
                / self.obj_type
                / ("part-{}".format(str(partition_id)))
            )
            node_set_dir.mkdir(exist_ok=True, parents=True)
            node_set_file = node_set_dir / "nodes-{}.db".format(obj_id_suffix)
            node_set = LevelDBSet(node_set_file)
            self.exit_stack.enter_context(node_set)
            self.node_sets[shard_id] = node_set
        return self.node_sets[shard_id]

    def run(self):
        """
        Start a Journal client on the given assignment and process all the
        incoming messages.
        """
        logger.debug("Start the JournalProcessorWorker")
        cfg = self.config["journal"].copy()
        cfg.update(
            object_types=[self.obj_type],
            group_id=self.group_id,
            debug="cgrp,broker",
            offset_ranges=self.offsets,
            assignment=self.assignment,
            progress_queue=self.progress_queue,
            **{"message.max.bytes": str(500 * 1024 * 1024)},
        )
        client = JournalClientOffsetRanges(**cfg)
        client.process(self.process_messages)

    def process_messages(self, messages):
        """
        Process the incoming Kafka messages.
        """
        for object_type, message_list in messages.items():
            fixed_objects_by_partition = collections.defaultdict(list)
            for message in message_list:
                fixed_objects_by_partition[message.partition()].extend(
                    zip(
                        [message.key()],
                        fix_objects(object_type, [kafka_to_value(message.value())]),
                    )
                )
            for partition, objects in fixed_objects_by_partition.items():
                for (key, obj) in objects:
                    self.process_message(object_type, partition, key, obj)

    def process_message(self, object_type, partition, obj_key, obj):
        """
        Process a single incoming Kafka message if the object it refers to has
        not been processed yet.

        It uses an on-disk set to make sure that each object is only ever
        processed once.
        """
        node_set = self.get_node_set_for_object(partition, obj_key)
        if not node_set.add(obj_key):
            # Node already processed, skipping.
            return

        for exporter in self.exporters:
            try:
                exporter.process_object(object_type, obj)
            except Exception:
                logger.exception(
                    "Exporter %s: error while exporting the object: %s",
                    exporter.__class__.__name__,
                    str(obj),
                )
