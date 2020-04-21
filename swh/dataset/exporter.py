# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import concurrent.futures
import multiprocessing
import time
import tqdm
from typing import Mapping, Sequence, Tuple
from concurrent.futures import FIRST_EXCEPTION, ProcessPoolExecutor
from confluent_kafka import TopicPartition

from swh.journal.client import JournalClient


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
        super().__init__(*args, **kwargs)

    def subscribe(self):
        self.topic_name = self.subscription[0]
        time.sleep(0.1)  # https://github.com/edenhill/librdkafka/issues/1983
        self.consumer.assign([
            TopicPartition(self.topic_name, pid) for pid in self.assignment
        ])

    def process(self, *args, **kwargs):
        self.count = 0
        try:
            self.handle_committed_offsets()
            super().process(*args, **kwargs)
        except EOFError:
            self.progress_queue.put(None)
            pass

    def handle_committed_offsets(self, ):
        """
        Handle already committed partition offsets before starting processing.
        """
        committed = self.consumer.committed([
            TopicPartition(self.topic_name, pid) for pid in self.assignment
        ])
        for tp in committed:
            self.handle_offset(tp.partition, tp.offset)

    def handle_offset(self, partition_id, offset):
        """
        Check whether the client has reached the end of the current
        partition, and trigger a reassignment if that is the case.
        Raise EOFError if all the partitions have reached the end.
        """
        if offset < 0:  # Uninitialized partition offset
            return

        if self.count % self.refresh_every == 0:
            self.progress_queue.put({partition_id: offset})

        if offset >= self.offset_ranges[partition_id][1] - 1:
            self.assignment = [pid for pid in self.assignment
                               if pid != partition_id]
            self.subscribe()

        if not self.assignment:
            raise EOFError

    def deserialize_message(self, message):
        """
        Override of the message deserialization to hook the handling of the
        message offset.
        """
        self.handle_offset(message.partition(), message.offset())
        self.count += 1
        return super().deserialize_message(message)


class ParallelExporter:
    """
    Base class for all the Journal exporters.

    Each exporter should override the `export_worker` function with an
    implementation of how to run the message processing.
    """
    def __init__(self, config, export_id: str, obj_type, processes=1):
        """
        Args:
            config: the exporter config, which should also include the
                JournalClient configuration.
            export_id: a unique identifier for the export that will be used
                as part of a Kafka consumer group ID.
            obj_type: The type of SWH object to export.
            processes: The number of processes to run.
        """
        self.config = config
        self.export_id = 'swh-dataset-export-{}'.format(export_id)
        self.obj_type = obj_type
        self.processes = processes
        self.offsets = None

    def get_offsets(self):
        """
        First pass to fetch all the current low and high offsets of each
        partition to define the consumption boundaries.
        """
        if self.offsets is None:
            client = JournalClient(
                **self.config['journal'],
                object_types=[self.obj_type],
                group_id=self.export_id,
            )
            topic_name = client.subscription[0]
            topics = client.consumer.list_topics(topic_name).topics
            partitions = topics[topic_name].partitions

            self.offsets = {}
            for partition_id in tqdm.tqdm(partitions.keys(),
                                          desc="  - Partition offsets"):
                tp = TopicPartition(topic_name, partition_id)
                (lo, hi) = client.consumer.get_watermark_offsets(tp)
                self.offsets[partition_id] = (lo, hi)
        return self.offsets

    def run(self, *args):
        """
        Run the parallel export.
        """
        self.get_offsets()
        to_assign = list(self.offsets.keys())

        manager = multiprocessing.Manager()
        q = manager.Queue()

        with ProcessPoolExecutor(self.processes + 1) as pool:
            futures = []
            for i in range(self.processes):
                futures.append(pool.submit(
                    self.export_worker,
                    *args,
                    assignment=to_assign[i::self.processes],
                    queue=q
                ))
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

    def progress_worker(self, *args, queue=None):
        """
        An additional worker process that reports the current progress of the
        export between all the different parallel consumers and across all the
        partitions, by consuming the shared progress reporting Queue.
        """
        d = {}
        active_workers = self.processes
        offset_diff = sum((hi - lo) for lo, hi in self.offsets.values())
        with tqdm.tqdm(total=offset_diff, desc="  - Journal export") as pbar:
            while active_workers:
                item = queue.get()
                if item is None:
                    active_workers -= 1
                    continue
                d.update(item)
                progress = sum(n - self.offsets[p][0] for p, n in d.items())
                pbar.set_postfix(active_workers=active_workers,
                                 total_workers=self.processes)
                pbar.update(progress - pbar.n)

    def process(self, callback, assignment=None, queue=None):
        client = JournalClientOffsetRanges(
            **self.config['journal'],
            object_types=[self.obj_type],
            group_id=self.export_id,
            debug='cgrp,broker',
            offset_ranges=self.offsets,
            assignment=assignment,
            progress_queue=queue,
        )
        client.process(callback)

    def export_worker(self, *args, **kwargs):
        """
        Override this with a custom implementation of a worker function.

        A worker function should call `self.process(fn, **kwargs)` with `fn`
        being a callback that will be called in the same fashion as with
        `JournalClient.process()`.

        A simple exporter to print all the objects in the log would look like
        this:

        ```
        class PrintExporter(ParallelExporter):
            def export_worker(self, **kwargs):
                self.process(print, **kwargs)
        ```
        """
        raise NotImplementedError
