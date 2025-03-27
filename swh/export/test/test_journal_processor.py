# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import gc
import multiprocessing
from multiprocessing.managers import ListProxy
from typing import Sequence, Tuple

import pytest

from swh.export.exporter import Exporter
from swh.export.journalprocessor import ParallelJournalProcessor
from swh.journal.writer import get_journal_writer
from swh.model import model
from swh.model.tests import swh_model_data


@pytest.fixture
def journal_client_config(
    kafka_server: str, kafka_prefix: str, kafka_consumer_group: str
):
    return dict(
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
    )


@pytest.fixture
def journal_writer(kafka_server: str, kafka_prefix: str):
    return get_journal_writer(
        cls="kafka",
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        anonymize=False,
    )


def disable_gc(f):
    """Decorator for test functions; prevents segfaults in confluent-kafka.
    See https://github.com/confluentinc/confluent-kafka-python/issues/1761"""

    @functools.wraps(f)
    def newf(*args, **kwargs):
        gc.disable()
        try:
            return f(*args, **kwargs)
        finally:
            gc.enable()

    return newf


class ListExporter(Exporter):
    def __init__(self, objects: ListProxy, *args, **kwargs):
        self._objects = objects
        super().__init__(*args, **kwargs)

    def process_object(
        self, object_type: model.ModelObjectType, obj: model.BaseModel
    ) -> None:
        self._objects.append((object_type, obj))


def assert_exported_objects(
    exported_objects: Sequence[Tuple[str, model.BaseModel]],
    expected_objects: Sequence[model.BaseModel],
) -> None:
    assert set(obj[1] for obj in exported_objects) == set(expected_objects)


def test_parallel_journal_processor(
    journal_client_config, journal_writer, tmp_path
) -> None:
    journal_writer.write_additions("revision", swh_model_data.REVISIONS)

    with multiprocessing.Manager() as manager:
        objects = manager.list()
        (tmp_path / "node_sets" / "revision").mkdir(parents=True)
        export_path = tmp_path / "export"
        sensitive_export_path = tmp_path / "sensitive" / "export"
        dup_dir = tmp_path / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = tmp_path / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)
        config = {"journal": journal_client_config}
        processor = ParallelJournalProcessor(
            config=config,
            masked_swhids=set(),
            exporter_factories=[
                functools.partial(
                    ListExporter,
                    objects,
                    config=config,
                    export_path=export_path,
                    sensitive_export_path=sensitive_export_path,
                )
            ],
            export_id="test_parallel_journal_processor",
            obj_type="revision",
            node_sets_path=tmp_path / "node_sets",
            dup_dir=dup_dir,
            dedup_dir=dedup_dir,
        )

        processor.run()

        assert_exported_objects(objects, swh_model_data.REVISIONS)


def test_parallel_journal_processor_origin(
    journal_client_config, journal_writer, tmp_path
) -> None:
    journal_writer.write_additions("origin", swh_model_data.ORIGINS)

    with multiprocessing.Manager() as manager:
        objects = manager.list()
        (tmp_path / "node_sets" / "origin").mkdir(parents=True)
        export_path = tmp_path / "export"
        sensitive_export_path = tmp_path / "sensitive" / "export"
        dup_dir = tmp_path / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = tmp_path / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)
        config = {"journal": journal_client_config}
        processor = ParallelJournalProcessor(
            config=config,
            masked_swhids=set(),
            exporter_factories=[
                functools.partial(
                    ListExporter,
                    objects,
                    config=config,
                    export_path=export_path,
                    sensitive_export_path=sensitive_export_path,
                )
            ],
            export_id="test_parallel_journal_processor",
            obj_type="origin",
            node_sets_path=tmp_path / "node_sets",
            dup_dir=dup_dir,
            dedup_dir=dedup_dir,
        )

        processor.run()

        assert_exported_objects(objects, swh_model_data.ORIGINS)


@disable_gc
def test_parallel_journal_processor_origin_visit_status(
    journal_client_config, journal_writer, tmp_path
) -> None:
    journal_writer.write_additions(
        "origin_visit_status", swh_model_data.ORIGIN_VISIT_STATUSES
    )

    with multiprocessing.Manager() as manager:
        objects = manager.list()
        (tmp_path / "node_sets" / "origin_visit_status").mkdir(parents=True)
        export_path = tmp_path / "export"
        sensitive_export_path = tmp_path / "sensitive" / "export"
        dup_dir = tmp_path / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = tmp_path / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)
        config = {"journal": journal_client_config}
        processor = ParallelJournalProcessor(
            config=config,
            masked_swhids=set(),
            exporter_factories=[
                functools.partial(
                    ListExporter,
                    objects,
                    config=config,
                    export_path=export_path,
                    sensitive_export_path=sensitive_export_path,
                )
            ],
            export_id="test_parallel_journal_processor",
            obj_type="origin_visit_status",
            node_sets_path=tmp_path / "node_sets",
            dup_dir=dup_dir,
            dedup_dir=dedup_dir,
        )

        processor.run()

        assert_exported_objects(objects, swh_model_data.ORIGIN_VISIT_STATUSES)


@disable_gc
def test_parallel_journal_processor_offsets(
    journal_client_config, journal_writer, tmp_path
) -> None:
    """Checks the exporter stops at the offsets computed at the beginning of the export"""
    journal_writer.write_additions("revision", swh_model_data.REVISIONS[0:2])

    with multiprocessing.Manager() as manager:
        objects = manager.list()
        (tmp_path / "node_sets" / "revision").mkdir(parents=True)
        export_path = tmp_path / "export"
        sensitive_export_path = tmp_path / "sensitive" / "export"
        dup_dir = tmp_path / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = tmp_path / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)
        config = {"journal": journal_client_config}
        processor = ParallelJournalProcessor(
            config=config,
            masked_swhids=set(),
            exporter_factories=[
                functools.partial(
                    ListExporter,
                    objects,
                    config=config,
                    export_path=export_path,
                    sensitive_export_path=sensitive_export_path,
                )
            ],
            export_id="test_parallel_journal_processor",
            obj_type="revision",
            node_sets_path=tmp_path / "node_sets",
            dup_dir=dup_dir,
            dedup_dir=dedup_dir,
        )

        processor.get_offsets()  # fills the processor.offsets cache

        processor.run()

        assert_exported_objects(objects, swh_model_data.REVISIONS[0:2])


@disable_gc
def test_parallel_journal_processor_masked(
    journal_client_config, journal_writer, tmp_path
) -> None:
    journal_writer.write_additions("revision", swh_model_data.REVISIONS)

    masked_swhids = {swh_model_data.REVISIONS[2].swhid().to_extended()}

    with multiprocessing.Manager() as manager:
        objects = manager.list()
        (tmp_path / "node_sets" / "revision").mkdir(parents=True)
        export_path = tmp_path / "export"
        sensitive_export_path = tmp_path / "sensitive" / "export"
        dup_dir = tmp_path / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = tmp_path / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)
        config = {"journal": journal_client_config}
        processor = ParallelJournalProcessor(
            config=config,
            masked_swhids=masked_swhids,
            exporter_factories=[
                functools.partial(
                    ListExporter,
                    objects,
                    config=config,
                    export_path=export_path,
                    sensitive_export_path=sensitive_export_path,
                )
            ],
            export_id="test_parallel_journal_processor",
            obj_type="revision",
            node_sets_path=tmp_path / "node_sets",
            dup_dir=dup_dir,
            dedup_dir=dedup_dir,
        )

        processor.run()

        assert_exported_objects(
            objects, swh_model_data.REVISIONS[0:2] + swh_model_data.REVISIONS[3:]
        )


@disable_gc
def test_parallel_journal_processor_masked_origin(
    journal_client_config, journal_writer, tmp_path
) -> None:
    journal_writer.write_additions("origin", swh_model_data.ORIGINS)

    masked_swhids = {swh_model_data.ORIGINS[1].swhid()}

    with multiprocessing.Manager() as manager:
        objects = manager.list()
        (tmp_path / "node_sets" / "origin").mkdir(parents=True)
        export_path = tmp_path / "export"
        sensitive_export_path = tmp_path / "sensitive" / "export"
        dup_dir = tmp_path / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = tmp_path / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)
        config = {"journal": journal_client_config}
        processor = ParallelJournalProcessor(
            config=config,
            masked_swhids=masked_swhids,
            exporter_factories=[
                functools.partial(
                    ListExporter,
                    objects,
                    config=config,
                    export_path=export_path,
                    sensitive_export_path=sensitive_export_path,
                )
            ],
            export_id="test_parallel_journal_processor",
            obj_type="origin",
            node_sets_path=tmp_path / "node_sets",
            dup_dir=dup_dir,
            dedup_dir=dedup_dir,
        )

        processor.run()

        assert_exported_objects(
            objects, swh_model_data.ORIGINS[0:1] + swh_model_data.ORIGINS[2:]
        )


@disable_gc
def test_parallel_journal_processor_masked_origin_visit_statuses(
    journal_client_config, journal_writer, tmp_path
) -> None:
    journal_writer.write_additions(
        "origin_visit_status", swh_model_data.ORIGIN_VISIT_STATUSES
    )

    masked_origin = model.Origin(url=swh_model_data.ORIGIN_VISIT_STATUSES[1].origin)
    masked_swhids = {masked_origin.swhid()}

    with multiprocessing.Manager() as manager:
        objects = manager.list()
        (tmp_path / "node_sets" / "origin_visit_status").mkdir(parents=True)
        export_path = tmp_path / "export"
        sensitive_export_path = tmp_path / "sensitive" / "export"
        dup_dir = tmp_path / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = tmp_path / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)
        config = {"journal": journal_client_config}
        processor = ParallelJournalProcessor(
            config=config,
            masked_swhids=masked_swhids,
            exporter_factories=[
                functools.partial(
                    ListExporter,
                    objects,
                    config=config,
                    export_path=export_path,
                    sensitive_export_path=sensitive_export_path,
                )
            ],
            export_id="test_parallel_journal_processor",
            obj_type="origin_visit_status",
            node_sets_path=tmp_path / "node_sets",
            dup_dir=dup_dir,
            dedup_dir=dedup_dir,
        )

        processor.run()

        assert_exported_objects(
            objects,
            [
                ovs
                for ovs in swh_model_data.ORIGIN_VISIT_STATUSES
                if ovs.origin != masked_origin.url
            ],
        )
