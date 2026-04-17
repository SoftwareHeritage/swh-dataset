# Copyright (C) 2020-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for the ORC and Parquet exporters"""

from contextlib import contextmanager
import math
from pathlib import Path
import tempfile
from typing import Any

import pyarrow.parquet
import pyorc
import pytest

from swh.export.exporters import orc, parquet, tabular
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
)
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.objstorage.factory import get_objstorage

from .utils import (
    assert_contents_exported,
    assert_directories_exported,
    assert_origin_visit_statuses_exported,
    assert_origin_visits_exported,
    assert_origins_exported,
    assert_releases_exported,
    assert_revisions_exported,
    assert_skipped_contents_exported,
    assert_snapshots_exported,
    load,
)


@contextmanager
def export_tmpdir(tmpdir):
    if tmpdir:
        yield Path(tmpdir)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)


@contextmanager
def export_sensitive_tmpdir(tmpdir):
    if tmpdir:
        yield Path(tmpdir)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)


@contextmanager
def export(
    messages, object_types, *, exporter, config=None, tmpdir=None, sensitive_tmpdir=None
):
    with export_tmpdir(tmpdir) as tmpdir:
        with export_sensitive_tmpdir(sensitive_tmpdir) as sensitive_tmpdir:
            if config is None:
                config = {}
            if exporter == "orc":
                with orc.ORCExporter(
                    config, object_types, tmpdir, sensitive_tmpdir
                ) as exporter:
                    for object_type, objects in messages.items():
                        for obj in objects:
                            exporter.process_object(object_type, obj)
                yield tmpdir
            else:
                assert exporter == "parquet"
                with parquet.ParquetExporter(
                    config, object_types, tmpdir, sensitive_tmpdir
                ) as exporter:
                    for object_type, objects in messages.items():
                        for obj in objects:
                            exporter.process_object(object_type, obj)
                yield tmpdir


@pytest.fixture(params=["orc", "parquet"])
def exporter_name(request):
    return request.param


@pytest.fixture
def exporter(exporter_name):
    def exporter(messages, object_types, config=None, tmpdir=None):
        with export(
            messages, object_types, exporter=exporter_name, config=config, tmpdir=tmpdir
        ) as exportdir:
            return load(exporter_name, exportdir)

    return exporter


def test_export_origin(exporter):
    obj_type = Origin.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_origins_exported(TEST_OBJECTS[obj_type], output["origin"])


def test_export_origin_visit(exporter):
    obj_type = OriginVisit.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_origin_visits_exported(TEST_OBJECTS[obj_type], output["origin_visit"])


def test_export_origin_visit_status(exporter):
    obj_type = OriginVisitStatus.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_origin_visit_statuses_exported(
        TEST_OBJECTS[obj_type], output["origin_visit_status"]
    )


def test_export_snapshot(exporter):
    obj_type = Snapshot.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_snapshots_exported(
        TEST_OBJECTS[obj_type], output["snapshot"], output["snapshot_branch"]
    )


def test_export_release(exporter):
    obj_type = Release.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_releases_exported(TEST_OBJECTS[obj_type], output["release"])


def test_export_revision(exporter):
    obj_type = Revision.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_revisions_exported(
        TEST_OBJECTS[obj_type], output["revision"], output["revision_history"]
    )


def test_export_directory(exporter):
    obj_type = Directory.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_directories_exported(
        TEST_OBJECTS[obj_type], output["directory"], output["directory_entry"]
    )


def test_export_content(exporter):
    obj_type = Content.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_contents_exported(TEST_OBJECTS[obj_type], output["content"])


def test_export_skipped_content(exporter):
    obj_type = SkippedContent.object_type
    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]}, [TEST_OBJECTS[obj_type][0].object_type.name]
    )
    assert_skipped_contents_exported(TEST_OBJECTS[obj_type], output["skipped_content"])


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
def test_export_related_files(
    exporter_name, exporter, max_rows, obj_type, tmpdir
) -> None:
    config: dict[str, Any] = {exporter_name: {}}
    if max_rows is not None:
        config[exporter_name]["max_rows"] = {obj_type.value: max_rows}
    exporter(
        {obj_type: TEST_OBJECTS[obj_type]},
        [TEST_OBJECTS[obj_type][0].object_type.name],
        config=config,
        tmpdir=tmpdir,
    )
    # check there are as many ORC files as objects
    paths = [
        fname
        for fname in (tmpdir / obj_type.value).listdir(f"{obj_type}-*.{exporter_name}")
    ]
    if max_rows is None:
        assert len(paths) == 1
    else:
        assert len(paths) == math.ceil(len(TEST_OBJECTS[obj_type]) / max_rows)
    # check the number of related ORC files
    related_tables = RELATED.get(obj_type.value, ())
    if not related_tables:
        # nothing to test
        return
    for related in related_tables:
        related_paths = [
            fname
            for fname in (tmpdir / related).listdir(f"{related}-*.{exporter_name}")
        ]
        assert len(related_paths) == len(paths)

    if exporter_name == "orc":
        # for each ORC file, check related files only reference objects in the
        # corresponding main table
        for path in paths:
            with path.open("rb") as orc_file:
                orc_reader = pyorc.Reader(
                    orc_file,
                    converters={pyorc.TypeKind.TIMESTAMP: orc.SWHTimestampConverter},
                )
                uuid = orc_reader.user_metadata["swh_uuid"].decode()
                assert path.basename == f"{obj_type}-{uuid}.orc"
                orc_rows = list(orc_reader)
                obj_ids = [row[0] for row in orc_rows]  # type: ignore[index]

            # check the related tables
            for related in RELATED.get(obj_type.value, ()):
                orc_path = tmpdir / related / f"{related}-{uuid}.orc"
                with orc_path.open("rb") as orc_file:
                    orc_reader = pyorc.Reader(
                        orc_file,
                        converters={
                            pyorc.TypeKind.TIMESTAMP: orc.SWHTimestampConverter
                        },
                    )
                    assert orc_reader.user_metadata["swh_uuid"].decode() == uuid
                    orc_rows = list(orc_reader)
                    # check branches in this file only concern current snapshot (obj_id)
                    for row in orc_rows:
                        assert row[0] in obj_ids  # type: ignore[index]
    else:
        assert exporter_name == "parquet"
        if obj_type == ModelObjectType.SNAPSHOT:
            key = "snapshot_id"
        elif obj_type == ModelObjectType.REVISION:
            key = "id"
        elif obj_type == ModelObjectType.DIRECTORY:
            key = "directory_id"
        else:
            assert False, f"Unexpected related table for {obj_type}"

        # for each Parquet file, check related files only reference objects in the
        # corresponding main table
        for path in paths:
            with path.open("rb") as parquet_file:
                parquet_reader = pyarrow.parquet.ParquetFile(parquet_file)
                metadata = parquet_reader.metadata.metadata
                assert metadata is not None
                uuid = metadata[b"swh_uuid"].decode()
                assert path.basename == f"{obj_type}-{uuid}.parquet"
                parquet_rows = parquet_reader.read().to_pylist()
                obj_ids = [row["id"] for row in parquet_rows]

            # check the related tables
            for related in RELATED.get(obj_type.value, ()):
                parquet_path = tmpdir / related / f"{related}-{uuid}.parquet"
                with parquet_path.open("rb") as parquet_file:
                    parquet_reader = pyarrow.parquet.ParquetFile(parquet_file)
                    metadata = parquet_reader.metadata.metadata
                    assert metadata is not None
                    assert metadata[b"swh_uuid"].decode() == uuid
                    # check branches in this file only concern current snapshot (obj_id)
                    for parquet_row in parquet_reader.read().to_pylist():
                        assert parquet_row[key] in obj_ids


@pytest.mark.parametrize(
    "obj_type",
    (ModelObjectType(obj_type) for obj_type in MAIN_TABLES.keys()),
)
def test_export_related_files_separated(exporter_name, exporter, obj_type, tmpdir):
    exporter(
        {obj_type: TEST_OBJECTS[obj_type]},
        [TEST_OBJECTS[obj_type][0].object_type.name],
        tmpdir=tmpdir,
    )
    # check there are as many ORC files as objects
    orcfiles = [
        fname
        for fname in (tmpdir / obj_type.value).listdir(f"{obj_type}-*.{exporter_name}")
    ]
    assert len(orcfiles) == 1
    # check related ORC files are in their own directory
    for related in RELATED.get(obj_type.value, ()):
        related_orcfiles = [
            fname
            for fname in (tmpdir / related).listdir(f"{related}-*.{exporter_name}")
        ]
        assert len(related_orcfiles) == len(orcfiles)


@pytest.mark.parametrize("table_name", RELATION_TABLES.keys())
def test_export_invalid_max_rows(exporter, table_name):
    subconfig = {"max_rows": {table_name: 10}}
    config = {"orc": subconfig, "parquet": subconfig}
    with pytest.raises(ValueError):
        exporter({}, [], config=config)


def test_export_content_with_data(exporter, monkeypatch, tmpdir):
    obj_type = Content.object_type
    objstorage = get_objstorage("memory")
    for content in TEST_OBJECTS[obj_type]:
        objstorage.add(content=content.data, obj_id=content.hashes())

    def get_objstorage_mock(**kw):
        if kw.get("cls") == "mock":
            return objstorage

    monkeypatch.setattr(tabular, "get_objstorage", get_objstorage_mock)
    subconfig = {
        "with_data": True,
        "objstorage": {"cls": "mock"},
    }
    config = {"orc": subconfig, "parquet": subconfig}

    output = exporter(
        {obj_type: TEST_OBJECTS[obj_type]},
        [TEST_OBJECTS[obj_type][0].object_type.name],
        config=config,
        tmpdir=tmpdir,
    )
    for obj in TEST_OBJECTS[obj_type]:
        assert (
            tabular.hash_to_hex_or_none(obj.sha1),
            tabular.hash_to_hex_or_none(obj.sha1_git),
            tabular.hash_to_hex_or_none(obj.sha256),
            tabular.hash_to_hex_or_none(obj.blake2s256),
            obj.length,
            obj.status,
            obj.data,
        ) in output[obj_type.value]
