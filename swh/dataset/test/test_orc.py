import collections
from pathlib import Path
import tempfile

import pyorc
import pytest

from swh.dataset.exporters.orc import (
    ORCExporter,
    hash_to_hex_or_none,
    swh_date_to_datetime,
    swh_date_to_offset,
)
from swh.model.tests.swh_model_data import TEST_OBJECTS


@pytest.fixture
def exporter():
    def wrapped(messages, config=None):
        with tempfile.TemporaryDirectory() as tmpname:
            tmppath = Path(tmpname)
            if config is None:
                config = {}
            with ORCExporter(config, tmppath) as exporter:
                for object_type, objects in messages.items():
                    for obj in objects:
                        exporter.process_object(object_type, obj.to_dict())
            res = collections.defaultdict(set)
            for obj_type_dir in tmppath.iterdir():
                for orc_file in obj_type_dir.iterdir():
                    with orc_file.open("rb") as orc_obj:
                        res[obj_type_dir.name] |= set(pyorc.Reader(orc_obj))
            return res

    return wrapped


def test_export_origin(exporter):
    obj_type = "origin"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (obj.url,) in output[obj_type]


def test_export_origin_visit(exporter):
    obj_type = "origin_visit"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (obj.origin, obj.visit, obj.date, obj.type) in output[obj_type]


def test_export_origin_visit_status(exporter):
    obj_type = "origin_visit_status"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            obj.origin,
            obj.visit,
            obj.date,
            obj.status,
            hash_to_hex_or_none(obj.snapshot),
        ) in output[obj_type]


def test_export_snapshot(exporter):
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


def test_export_release(exporter):
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
            swh_date_to_datetime(obj.date.to_dict()) if obj.date else None,
            swh_date_to_offset(obj.date.to_dict()) if obj.date else None,
        ) in output[obj_type]


def test_export_revision(exporter):
    obj_type = "revision"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            hash_to_hex_or_none(obj.id),
            obj.message,
            obj.author.fullname,
            swh_date_to_datetime(obj.date.to_dict()),
            swh_date_to_offset(obj.date.to_dict()),
            obj.committer.fullname,
            swh_date_to_datetime(obj.committer_date.to_dict()),
            swh_date_to_offset(obj.committer_date.to_dict()),
            hash_to_hex_or_none(obj.directory),
        ) in output["revision"]
        for i, parent in enumerate(obj.parents):
            assert (
                hash_to_hex_or_none(obj.id),
                hash_to_hex_or_none(parent),
                i,
            ) in output["revision_history"]


def test_export_directory(exporter):
    obj_type = "directory"
    output = exporter({obj_type: TEST_OBJECTS[obj_type]})
    for obj in TEST_OBJECTS[obj_type]:
        assert (hash_to_hex_or_none(obj.id),) in output["directory"]
        for entry in obj.entries:
            assert (
                hash_to_hex_or_none(obj.id),
                entry.name,
                entry.type,
                hash_to_hex_or_none(entry.target),
                entry.perms,
            ) in output["directory_entry"]


def test_export_content(exporter):
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


def test_export_skipped_content(exporter):
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
