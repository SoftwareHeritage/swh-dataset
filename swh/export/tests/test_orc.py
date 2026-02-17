# Copyright (C) 2020-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import math
from pathlib import Path
import tempfile

import pyorc
import pytest

from swh.export.exporters import orc
from swh.export.relational import MAIN_TABLES, RELATION_TABLES
from swh.model.model import (
    Content,
    Directory,
    ModelObjectType,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
    TimestampWithTimezone,
)
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.objstorage.factory import get_objstorage

from .utils import (
    assert_contents_exported_to_orc,
    assert_directories_exported_to_orc,
    assert_origin_visit_statuses_exported_to_orc,
    assert_origin_visits_exported_to_orc,
    assert_origins_exported_to_orc,
    assert_releases_exported_to_orc,
    assert_revisions_exported_to_orc,
    assert_skipped_contents_exported_to_orc,
    assert_snapshots_exported_to_orc,
    orc_load,
)


@contextmanager
def orc_tmpdir(tmpdir):
    if tmpdir:
        yield Path(tmpdir)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)


@contextmanager
def orc_sensitive_tmpdir(tmpdir):
    if tmpdir:
        yield Path(tmpdir)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)


@contextmanager
def orc_export(messages, object_types, config=None, tmpdir=None, sensitive_tmpdir=None):
    with orc_tmpdir(tmpdir) as tmpdir:
        with orc_sensitive_tmpdir(sensitive_tmpdir) as sensitive_tmpdir:
            if config is None:
                config = {}
            with orc.ORCExporter(
                config, object_types, tmpdir, sensitive_tmpdir
            ) as exporter:
                for object_type, objects in messages.items():
                    for obj in objects:
                        exporter.process_object(object_type, obj)
            yield tmpdir


def exporter(messages, object_types, config=None, tmpdir=None):
    with orc_export(messages, object_types, config, tmpdir) as exportdir:
        return orc_load(exportdir)


def test_export_origin():
    obj_type = Origin.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_origins_exported_to_orc(TEST_OBJECTS[obj_type], output["origin"])


def test_export_origin_visit():
    obj_type = OriginVisit.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_origin_visits_exported_to_orc(TEST_OBJECTS[obj_type], output["origin_visit"])


def test_export_origin_visit_status():
    obj_type = OriginVisitStatus.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_origin_visit_statuses_exported_to_orc(
        TEST_OBJECTS[obj_type], output["origin_visit_status"]
    )


def test_export_snapshot():
    obj_type = Snapshot.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_snapshots_exported_to_orc(
        TEST_OBJECTS[obj_type], output["snapshot"], output["snapshot_branch"]
    )


def test_export_release():
    obj_type = Release.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_releases_exported_to_orc(TEST_OBJECTS[obj_type], output["release"])


def test_export_revision():
    obj_type = Revision.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_revisions_exported_to_orc(
        TEST_OBJECTS[obj_type], output["revision"], output["revision_history"]
    )


def test_export_directory():
    obj_type = Directory.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_directories_exported_to_orc(
        TEST_OBJECTS[obj_type], output["directory"], output["directory_entry"]
    )


def test_export_content():
    obj_type = Content.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_contents_exported_to_orc(TEST_OBJECTS[obj_type], output["content"])


def test_export_skipped_content():
    obj_type = SkippedContent.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_skipped_contents_exported_to_orc(
        TEST_OBJECTS[obj_type], output["skipped_content"]
    )


def test_date_to_tuple():
    ts = {"seconds": 123456, "microseconds": 1515}
    assert orc.swh_date_to_tuple(
        TimestampWithTimezone.from_dict({"timestamp": ts, "offset_bytes": b"+0100"})
    ) == (
        (123456, 1515),
        60,
        b"+0100",
    )

    assert orc.swh_date_to_tuple(
        TimestampWithTimezone.from_dict(
            {
                "timestamp": ts,
                "offset": 120,
                "negative_utc": False,
                "offset_bytes": b"+0100",
            }
        )
    ) == ((123456, 1515), 60, b"+0100")

    assert orc.swh_date_to_tuple(
        TimestampWithTimezone.from_dict(
            {
                "timestamp": ts,
                "offset": 120,
                "negative_utc": False,
            }
        )
    ) == ((123456, 1515), 120, b"+0200")

    assert orc.swh_date_to_tuple(
        TimestampWithTimezone.from_dict(
            {
                "timestamp": ts,
                "offset": 0,
                "negative_utc": True,
            }
        )
    ) == (
        (123456, 1515),
        0,
        b"-0000",
    )


# mapping of related tables for each main table (if any)
RELATED = {
    "snapshot": ["snapshot_branch"],
    "revision": ["revision_history", "revision_extra_headers"],
    "directory": ["directory_entry"],
}


@pytest.mark.parametrize(
    "obj_type",
    (ModelObjectType(obj_type) for obj_type in MAIN_TABLES.keys()),
)
@pytest.mark.parametrize("max_rows", (None, 1, 2, 10000))
def test_export_related_files(max_rows, obj_type, tmpdir):
    config = {"orc": {}}
    if max_rows is not None:
        config["orc"]["max_rows"] = {obj_type.value: max_rows}
    exporter(
        {obj_type: TEST_OBJECTS[obj_type]},
        [TEST_OBJECTS[obj_type][0].object_type.name],
        config=config,
        tmpdir=tmpdir,
    )
    # check there are as many ORC files as objects
    orcfiles = [
        fname for fname in (tmpdir / obj_type.value).listdir(f"{obj_type}-*.orc")
    ]
    if max_rows is None:
        assert len(orcfiles) == 1
    else:
        assert len(orcfiles) == math.ceil(len(TEST_OBJECTS[obj_type]) / max_rows)
    # check the number of related ORC files
    for related in RELATED.get(obj_type.value, ()):
        related_orcfiles = [
            fname for fname in (tmpdir / related).listdir(f"{related}-*.orc")
        ]
        assert len(related_orcfiles) == len(orcfiles)

    # for each ORC file, check related files only reference objects in the
    # corresponding main table
    for orc_file in orcfiles:
        with orc_file.open("rb") as orc_obj:
            reader = pyorc.Reader(
                orc_obj,
                converters={pyorc.TypeKind.TIMESTAMP: orc.SWHTimestampConverter},
            )
            uuid = reader.user_metadata["swh_uuid"].decode()
            assert orc_file.basename == f"{obj_type}-{uuid}.orc"
            rows = list(reader)
            obj_ids = [row[0] for row in rows]

        # check the related tables
        for related in RELATED.get(obj_type.value, ()):
            orc_file = tmpdir / related / f"{related}-{uuid}.orc"
            with orc_file.open("rb") as orc_obj:
                reader = pyorc.Reader(
                    orc_obj,
                    converters={pyorc.TypeKind.TIMESTAMP: orc.SWHTimestampConverter},
                )
                assert reader.user_metadata["swh_uuid"].decode() == uuid
                rows = list(reader)
                # check branches in this file only concern current snapshot (obj_id)
                for row in rows:
                    assert row[0] in obj_ids


@pytest.mark.parametrize(
    "obj_type",
    (ModelObjectType(obj_type) for obj_type in MAIN_TABLES.keys()),
)
def test_export_related_files_separated(obj_type, tmpdir):
    exporter(
        {obj_type: TEST_OBJECTS[obj_type]},
        [TEST_OBJECTS[obj_type][0].object_type.name],
        tmpdir=tmpdir,
    )
    # check there are as many ORC files as objects
    orcfiles = [
        fname for fname in (tmpdir / obj_type.value).listdir(f"{obj_type}-*.orc")
    ]
    assert len(orcfiles) == 1
    # check related ORC files are in their own directory
    for related in RELATED.get(obj_type.value, ()):
        related_orcfiles = [
            fname for fname in (tmpdir / related).listdir(f"{related}-*.orc")
        ]
        assert len(related_orcfiles) == len(orcfiles)


@pytest.mark.parametrize("table_name", RELATION_TABLES.keys())
def test_export_invalid_max_rows(table_name):
    config = {"orc": {"max_rows": {table_name: 10}}}
    with pytest.raises(ValueError):
        exporter({}, [], config=config)


def test_export_content_with_data(monkeypatch, tmpdir):
    obj_type = Content.object_type
    objstorage = get_objstorage("memory")
    for content in TEST_OBJECTS[obj_type]:
        objstorage.add(content=content.data, obj_id=content.hashes())

    def get_objstorage_mock(**kw):
        if kw.get("cls") == "mock":
            return objstorage

    monkeypatch.setattr(orc, "get_objstorage", get_objstorage_mock)
    config = {
        "orc": {
            "with_data": True,
            "objstorage": {"cls": "mock"},
        },
    }

    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]},
        [TEST_OBJECTS[obj_type][0].object_type.name],
        config=config,
        tmpdir=tmpdir,
    )
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            orc.hash_to_hex_or_none(obj.sha1),
            orc.hash_to_hex_or_none(obj.sha1_git),
            orc.hash_to_hex_or_none(obj.sha256),
            orc.hash_to_hex_or_none(obj.blake2s256),
            obj.length,
            obj.status,
            obj.data,
        ) in output[obj_type.value]
