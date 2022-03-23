# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
from contextlib import contextmanager
import math
from pathlib import Path
import tempfile

import pyorc
import pytest

from swh.dataset.exporters.orc import (
    ORCExporter,
    SWHTimestampConverter,
    datetime_to_tuple,
    hash_to_hex_or_none,
    swh_date_to_tuple,
)
from swh.dataset.relational import MAIN_TABLES, RELATION_TABLES
from swh.model.tests.swh_model_data import TEST_OBJECTS


@contextmanager
def orc_tmpdir(tmpdir):
    if tmpdir:
        yield Path(tmpdir)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)


@contextmanager
def orc_export(messages, config=None, tmpdir=None):

    with orc_tmpdir(tmpdir) as tmpdir:
        if config is None:
            config = {}
        with ORCExporter(config, tmpdir) as exporter:
            for object_type, objects in messages.items():
                for obj in objects:
                    exporter.process_object(object_type, obj.to_dict())
        yield tmpdir


def orc_load(rootdir):
    res = collections.defaultdict(list)
    res["rootdir"] = rootdir
    for obj_type_dir in rootdir.iterdir():
        for orc_file in obj_type_dir.iterdir():
            with orc_file.open("rb") as orc_obj:
                reader = pyorc.Reader(
                    orc_obj,
                    converters={pyorc.TypeKind.TIMESTAMP: SWHTimestampConverter},
                )
                obj_type = reader.user_metadata["swh_object_type"].decode()
                res[obj_type].extend(reader)
    return res


def exporter(messages, config=None, tmpdir=None):
    with orc_export(messages, config, tmpdir) as exportdir:
        return orc_load(exportdir)


def test_export_origin():
    obj_type = "origin"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (obj.url,) in output[obj_type]


def test_export_origin_visit():
    obj_type = "origin_visit"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (obj.origin, obj.visit, datetime_to_tuple(obj.date), obj.type) in output[
            obj_type
        ]


def test_export_origin_visit_status():
    obj_type = "origin_visit_status"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            obj.origin,
            obj.visit,
            datetime_to_tuple(obj.date),
            obj.status,
            hash_to_hex_or_none(obj.snapshot),
            obj.type,
        ) in output[obj_type]


def test_export_snapshot():
    obj_type = "snapshot"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (hash_to_hex_or_none(obj.id),) in output["snapshot"]
        for branch_name, branch in obj.branches.items():
            if branch is None:
                continue
            assert (
                hash_to_hex_or_none(obj.id),
                branch_name,
                hash_to_hex_or_none(branch.target),
                str(branch.target_type.value),
            ) in output["snapshot_branch"]


def test_export_release():
    obj_type = "release"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            hash_to_hex_or_none(obj.id),
            obj.name,
            obj.message,
            hash_to_hex_or_none(obj.target),
            obj.target_type.value,
            obj.author.fullname if obj.author else None,
            *swh_date_to_tuple(obj.date.to_dict() if obj.date is not None else None),
            obj.raw_manifest,
        ) in output[obj_type]


def test_export_revision():
    obj_type = "revision"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            hash_to_hex_or_none(obj.id),
            obj.message,
            obj.author.fullname,
            *swh_date_to_tuple(obj.date.to_dict() if obj.date is not None else None),
            obj.committer.fullname,
            *swh_date_to_tuple(
                obj.committer_date.to_dict() if obj.committer_date is not None else None
            ),
            hash_to_hex_or_none(obj.directory),
            obj.type.value,
            obj.raw_manifest,
        ) in output["revision"]
        for i, parent in enumerate(obj.parents):
            assert (
                hash_to_hex_or_none(obj.id),
                hash_to_hex_or_none(parent),
                i,
            ) in output["revision_history"]


def test_export_directory():
    obj_type = "directory"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (hash_to_hex_or_none(obj.id), obj.raw_manifest) in output["directory"]
        for entry in obj.entries:
            assert (
                hash_to_hex_or_none(obj.id),
                entry.name,
                entry.type,
                hash_to_hex_or_none(entry.target),
                entry.perms,
            ) in output["directory_entry"]


def test_export_content():
    obj_type = "content"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            hash_to_hex_or_none(obj.sha1),
            hash_to_hex_or_none(obj.sha1_git),
            hash_to_hex_or_none(obj.sha256),
            hash_to_hex_or_none(obj.blake2s256),
            obj.length,
            obj.status,
        ) in output[obj_type]


def test_export_skipped_content():
    obj_type = "skipped_content"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            hash_to_hex_or_none(obj.sha1),
            hash_to_hex_or_none(obj.sha1_git),
            hash_to_hex_or_none(obj.sha256),
            hash_to_hex_or_none(obj.blake2s256),
            obj.length,
            obj.status,
            obj.reason,
        ) in output[obj_type]


def test_date_to_tuple():
    ts = {"seconds": 123456, "microseconds": 1515}
    assert swh_date_to_tuple({"timestamp": ts, "offset_bytes": b"+0100"}) == (
        (123456, 1515),
        60,
        b"+0100",
    )

    assert swh_date_to_tuple(
        {
            "timestamp": ts,
            "offset": 120,
            "negative_utc": False,
            "offset_bytes": b"+0100",
        }
    ) == ((123456, 1515), 60, b"+0100")

    assert swh_date_to_tuple(
        {"timestamp": ts, "offset": 120, "negative_utc": False,}
    ) == ((123456, 1515), 120, b"+0200")

    assert swh_date_to_tuple({"timestamp": ts, "offset": 0, "negative_utc": True,}) == (
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
    "obj_type", MAIN_TABLES.keys(),
)
@pytest.mark.parametrize("max_rows", (None, 1, 2, 10000))
def test_export_related_files(max_rows, obj_type, tmpdir):
    config = {}
    if max_rows is not None:
        config["max_rows"] = {obj_type: max_rows}
    exporter({obj_type: TEST_OBJECTS[obj_type]}, config=config, tmpdir=tmpdir)
    # check there are as many ORC files as objects
    orcfiles = [fname for fname in (tmpdir / obj_type).listdir(f"{obj_type}-*.orc")]
    if max_rows is None:
        assert len(orcfiles) == 1
    else:
        assert len(orcfiles) == math.ceil(len(TEST_OBJECTS[obj_type]) / max_rows)
    # check the number of related ORC files
    for related in RELATED.get(obj_type, ()):
        related_orcfiles = [
            fname for fname in (tmpdir / obj_type).listdir(f"{related}-*.orc")
        ]
        assert len(related_orcfiles) == len(orcfiles)

    # for each ORC file, check related files only reference objects in the
    # corresponding main table
    for orc_file in orcfiles:
        with orc_file.open("rb") as orc_obj:
            reader = pyorc.Reader(
                orc_obj, converters={pyorc.TypeKind.TIMESTAMP: SWHTimestampConverter},
            )
            uuid = reader.user_metadata["swh_uuid"].decode()
            assert orc_file.basename == f"{obj_type}-{uuid}.orc"
            rows = list(reader)
            obj_ids = [row[0] for row in rows]

        # check the related tables
        for related in RELATED.get(obj_type, ()):
            orc_file = tmpdir / obj_type / f"{related}-{uuid}.orc"
            with orc_file.open("rb") as orc_obj:
                reader = pyorc.Reader(
                    orc_obj,
                    converters={pyorc.TypeKind.TIMESTAMP: SWHTimestampConverter},
                )
                assert reader.user_metadata["swh_uuid"].decode() == uuid
                rows = list(reader)
                # check branches in this file only concern current snapshot (obj_id)
                for row in rows:
                    assert row[0] in obj_ids


@pytest.mark.parametrize("table_name", RELATION_TABLES.keys())
def test_export_invalid_max_rows(table_name):
    config = {"max_rows": {table_name: 10}}
    with pytest.raises(ValueError):
        exporter({}, config=config)
