import concurrent.futures
import multiprocessing
import tqdm
import time
from concurrent.futures import FIRST_EXCEPTION, ProcessPoolExecutor
from confluent_kafka import TopicPartition

from swh.journal.client import JournalClient


class JournalClientOffsetRanges(JournalClient):
    def __init__(self, *args, offset_ranges=None, assignment=None,
                 progress_queue=None, refresh_every=200, **kwargs):
        self.offset_ranges = offset_ranges
        self.progress_queue = progress_queue
        self.refresh_every = refresh_every
        self.assignment = assignment
        super().__init__(*args, **kwargs)

    def subscribe(self):
        topic_name = self.subscription[0]
        time.sleep(0.1)  # https://github.com/edenhill/librdkafka/issues/1983
        self.consumer.assign([
            TopicPartition(topic_name, pid) for pid in self.assignment
        ])

    def process(self, *args, **kwargs):
        self.count = 0
        try:
            # Handle already committed partition offsets
            topic_name = self.subscription[0]
            committed = self.consumer.committed([
                TopicPartition(topic_name, pid) for pid in self.assignment
            ])
            for tp in committed:
                self.handle_offset(tp.partition, tp.offset)

            if not self.assignment:
                raise EOFError

            # Process the messages
            super().process(*args, **kwargs)
        except EOFError:
            self.progress_queue.put(None)
            pass

    def handle_offset(self, partition_id, offset):
        if offset < 0:  # Uninitialized partition offset
            return

        if self.count % self.refresh_every == 0:
            self.progress_queue.put({partition_id: offset})

        if offset >= self.offset_ranges[partition_id][1] - 1:
            self.assignment = [pid for pid in self.assignment
                               if pid != partition_id]
            self.subscribe()

    def deserialize_message(self, message):
        self.handle_offset(message.partition(), message.offset())
        self.count += 1

        if not self.assignment:
            raise EOFError

        return super().deserialize_message(message)


class ParallelExporter:
    def __init__(self, config, export_id, obj_type, processes=1):
        self.config = config
        self.export_id = 'swh-dataset-export-{}'.format(export_id)
        self.obj_type = obj_type
        self.processes = processes

        self.offsets = None

    def get_offsets(self):
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
                    print(exc)
                    f.result()
                    raise exc

    def progress_worker(self, *args, queue=None):
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
