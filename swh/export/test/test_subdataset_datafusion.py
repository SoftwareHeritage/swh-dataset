# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for generate_subdataset_datafusion function."""

import hashlib
from pathlib import Path
import os
import tempfile

import pyarrow as pa
import pyarrow.orc as orc
import pytest

from swh.export.athena import generate_subdataset_datafusion
from swh.export.relational import TABLES


def table_schema_to_pyarrow(table_name: str) -> pa.Schema:
    """Convert a table schema from relational.py to PyArrow schema."""
    type_mapping: dict[str, pa.DataType] = {
        "string": pa.string(),
        "bigint": pa.int64(),
        "timestamp": pa.timestamp("ns"),
        "binary": pa.binary(),
        "smallint": pa.int16(),
        "int": pa.int32(),
    }
    fields = []
    for field_name, field_type in TABLES[table_name]:
        fields.append((field_name, type_mapping[field_type]))
    return pa.schema(fields)


def write_orc_table(path: str, schema: pa.Schema, rows: list[tuple]):
    """Write an ORC file with the given schema and rows."""
    arrays = []
    for i, field in enumerate(schema):
        values = [row[i] for row in rows]
        arrays.append(pa.array(values, type=field.type))
    table = pa.Table.from_arrays(arrays, schema=schema)
    orc.write_table(table, path)


def write_table_dataset(dataset_path: Path, table_name: str, rows: list[tuple]):
    """Write an ORC table to the dataset directory."""
    table_dir = dataset_path / table_name
    table_dir.mkdir(parents=True)
    schema = table_schema_to_pyarrow(table_name)
    write_orc_table(str(table_dir / f"{table_name}.orc"), schema, rows)


@pytest.fixture(scope="session")
def test_dataset(tmpdir_factory):
    """Create a test ORC dataset with known data."""
    dataset_path = Path(tmpdir_factory.mktemp("dataset"))

    url1 = "https://example.com/repo1"
    url2 = "https://example.com/repo2"

    write_table_dataset(dataset_path, "origin", [
        (hashlib.sha1(url1.encode()).hexdigest(), url1),
        (hashlib.sha1(url2.encode()).hexdigest(), url2),
    ])

    write_table_dataset(dataset_path, "origin_visit", [
        (url1, 1, None, "git"),
        (url1, 2, None, "git"),
        (url2, 1, None, "git"),
    ])

    write_table_dataset(dataset_path, "origin_visit_status", [
        (url1, 1, None, "full", "0000000000000000000000000000000000000020", "git"),
        (url1, 2, None, "full", "0000000000000000000000000000000000000020", "git"),
        (url2, 1, None, "full", "0000000000000000000000000000000000000021", "git"),
    ])

    write_table_dataset(dataset_path, "snapshot", [
        ("0000000000000000000000000000000000000020",),
        ("0000000000000000000000000000000000000021",),
    ])

    write_table_dataset(dataset_path, "snapshot_branch", [
        ("0000000000000000000000000000000000000020", b"refs/heads/main", "0000000000000000000000000000000000000032", "revision"),
        ("0000000000000000000000000000000000000020", b"refs/tags/v1.0", "0000000000000000000000000000000000000040", "release"),
        ("0000000000000000000000000000000000000021", b"refs/heads/main", "0000000000000000000000000000000000000031", "revision"),
    ])

    write_table_dataset(dataset_path, "release", [
        ("0000000000000000000000000000000000000040", b"v1.0", b"Release 1.0", "0000000000000000000000000000000000000030", "revision", b"Author <a@example.com>", None, 0, b"+0000", None),
        ("0000000000000000000000000000000000000041", b"v2.0", b"Release 2.0", "0000000000000000000000000000000000000032", "revision", b"Author <a@example.com>", None, 0, b"+0000", None),
    ])

    write_table_dataset(dataset_path, "revision", [
        ("0000000000000000000000000000000000000030", b"Initial commit", b"Author <a@example.com>", None, 0, b"+0000", b"Committer <c@example.com>", None, 0, b"+0000", "0000000000000000000000000000000000000010", "git", None),
        ("0000000000000000000000000000000000000031", b"Second commit", b"Author <a@example.com>", None, 0, b"+0000", b"Committer <c@example.com>", None, 0, b"+0000", "0000000000000000000000000000000000000011", "git", None),
        ("0000000000000000000000000000000000000032", b"Third commit", b"Author <a@example.com>", None, 0, b"+0000", b"Committer <c@example.com>", None, 0, b"+0000", "0000000000000000000000000000000000000012", "git", None),
    ])

    write_table_dataset(dataset_path, "revision_history", [
        ("0000000000000000000000000000000000000031", "0000000000000000000000000000000000000030", 0),
        ("0000000000000000000000000000000000000032", "0000000000000000000000000000000000000031", 0),
    ])

    write_table_dataset(dataset_path, "revision_extra_headers", [
        ("0000000000000000000000000000000000000030", b"gpgsig", b"-----BEGIN PGP SIGNATURE-----"),
        ("0000000000000000000000000000000000000031", b"gpgsig", b"-----BEGIN PGP SIGNATURE-----"),
    ])

    write_table_dataset(dataset_path, "directory", [
        ("0000000000000000000000000000000000000010", None),
        ("0000000000000000000000000000000000000011", None),
        ("0000000000000000000000000000000000000012", None),
    ])

    write_table_dataset(dataset_path, "directory_entry", [
        ("0000000000000000000000000000000000000010", b"file1.txt", "file", "0000000000000000000000000000000000000001", 0o100644),
        ("0000000000000000000000000000000000000010", b"file2.txt", "file", "0000000000000000000000000000000000000002", 0o100644),
        ("0000000000000000000000000000000000000011", b"subdir", "dir", "0000000000000000000000000000000000000012", 0o040000),
        ("0000000000000000000000000000000000000012", b"file3.txt", "file", "0000000000000000000000000000000000000003", 0o100755),
    ])

    write_table_dataset(dataset_path, "content", [
        ("sha1_a", "0000000000000000000000000000000000000001", "sha256_a", "blake_a", 100, "visible", None),
        ("sha1_b", "0000000000000000000000000000000000000002", "sha256_b", "blake_b", 200, "visible", None),
        ("sha1_c", "0000000000000000000000000000000000000003", "sha256_c", "blake_c", 300, "visible", None),
        ("sha1_d", "0000000000000000000000000000000000000004", "sha256_d", "blake_d", 400, "visible", None),
    ])

    write_table_dataset(dataset_path, "skipped_content", [
        ("skip_sha1_a", "0000000000000000000000000000000000000005", "skip_sha256_a", "skip_blake_a", 500, "absent", "too large"),
        ("skip_sha1_b", "0000000000000000000000000000000000000006", "skip_sha256_b", "skip_blake_b", 600, "absent", "too large"),
    ])

    return dataset_path, {
        "url1": url1,
        "url2": url2,
        "url1_hash": hashlib.sha1(url1.encode()).hexdigest(),
        "url2_hash": hashlib.sha1(url2.encode()).hexdigest(),
    }


def test_filter_origin(test_dataset, tmp_path):
    """Test filtering origin, origin_visit, and origin_visit_status tables by SWHID."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text(f"swh:1:ori:{data['url1_hash']}\n")

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    origin_out = orc.read_table(str(output_path / "origin" / "origin.orc"))
    assert origin_out.num_rows == 1
    assert origin_out["url"][0].as_py() == data["url1"]

    origin_visit_out = orc.read_table(str(output_path / "origin_visit" / "origin_visit.orc"))
    assert origin_visit_out.num_rows == 2
    assert all(val.as_py() == data["url1"] for val in origin_visit_out["origin"])

    origin_visit_status_out = orc.read_table(str(output_path / "origin_visit_status" / "origin_visit_status.orc"))
    assert origin_visit_status_out.num_rows == 2
    assert all(val.as_py() == data["url1"] for val in origin_visit_status_out["origin"])


def test_filter_snapshot(test_dataset, tmp_path):
    """Test filtering snapshot and snapshot_branch tables by SWHID."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text("swh:1:snp:0000000000000000000000000000000000000020\n")

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    snapshot_out = orc.read_table(str(output_path / "snapshot" / "snapshot.orc"))
    assert snapshot_out.num_rows == 1
    assert snapshot_out["id"][0].as_py() == "0000000000000000000000000000000000000020"

    snapshot_branch_out = orc.read_table(str(output_path / "snapshot_branch" / "snapshot_branch.orc"))
    assert snapshot_branch_out.num_rows == 2
    assert all(val.as_py() == "0000000000000000000000000000000000000020" for val in snapshot_branch_out["snapshot_id"])


def test_filter_release(test_dataset, tmp_path):
    """Test filtering release table by SWHID."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text("swh:1:rel:0000000000000000000000000000000000000040\n")

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    out = orc.read_table(str(output_path / "release" / "release.orc"))
    assert out.num_rows == 1
    assert out["id"][0].as_py() == "0000000000000000000000000000000000000040"


def test_filter_revision(test_dataset, tmp_path):
    """Test filtering revision, revision_history, and revision_extra_headers tables by SWHID."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text(
        "swh:1:rev:0000000000000000000000000000000000000030\n"
        "swh:1:rev:0000000000000000000000000000000000000031\n"
    )

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    revision_out = orc.read_table(str(output_path / "revision" / "revision.orc"))
    assert revision_out.num_rows == 2
    assert set(revision_out["id"].to_pylist()) == {
        "0000000000000000000000000000000000000030",
        "0000000000000000000000000000000000000031",
    }

    revision_history_out = orc.read_table(str(output_path / "revision_history" / "revision_history.orc"))
    assert revision_history_out.num_rows == 1
    assert revision_history_out["id"][0].as_py() == "0000000000000000000000000000000000000031"
    assert revision_history_out["parent_id"][0].as_py() == "0000000000000000000000000000000000000030"

    revision_extra_headers_out = orc.read_table(str(output_path / "revision_extra_headers" / "revision_extra_headers.orc"))
    assert revision_extra_headers_out.num_rows == 2
    assert set(revision_extra_headers_out["id"].to_pylist()) == {
        "0000000000000000000000000000000000000030",
        "0000000000000000000000000000000000000031",
    }


def test_filter_directory(test_dataset, tmp_path):
    """Test filtering directory and directory_entry tables by SWHID."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text(
        "swh:1:dir:0000000000000000000000000000000000000010\n"
        "swh:1:dir:0000000000000000000000000000000000000012\n"
    )

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    directory_out = orc.read_table(str(output_path / "directory" / "directory.orc"))
    assert directory_out.num_rows == 2
    assert set(directory_out["id"].to_pylist()) == {
        "0000000000000000000000000000000000000010",
        "0000000000000000000000000000000000000012",
    }

    directory_entry_out = orc.read_table(str(output_path / "directory_entry" / "directory_entry.orc"))
    assert directory_entry_out.num_rows == 3
    assert set(directory_entry_out["directory_id"].to_pylist()) == {
        "0000000000000000000000000000000000000010",
        "0000000000000000000000000000000000000012",
    }


def test_filter_content(test_dataset, tmp_path):
    """Test filtering content table by SWHID."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text(
        "swh:1:cnt:0000000000000000000000000000000000000001\n"
        "swh:1:cnt:0000000000000000000000000000000000000003\n"
    )

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    content_out = orc.read_table(str(output_path / "content" / "content.orc"))
    assert content_out.num_rows == 2
    assert set(content_out["sha1_git"].to_pylist()) == {
        "0000000000000000000000000000000000000001",
        "0000000000000000000000000000000000000003",
    }


def test_filter_skipped_content(test_dataset, tmp_path):
    """Test filtering skipped_content table by SWHID."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text("swh:1:cnt:0000000000000000000000000000000000000005\n")

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    out = orc.read_table(str(output_path / "skipped_content" / "skipped_content.orc"))
    assert out.num_rows == 1
    assert out["sha1_git"][0].as_py() == "0000000000000000000000000000000000000005"


def test_empty_result(test_dataset, tmp_path):
    """Test when no SWHIDs match."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"

    swhids_file.write_text("swh:1:cnt:ffffffffffffffffffffffffffffffffffffffff\n")

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    assert not (output_path / "content" / "content.orc").exists()


def test_all_tables_filtered(test_dataset, tmp_path):
    """Test filtering all tables in a single run."""
    dataset_path, data = test_dataset
    output_path = tmp_path / "output"
    swhids_file = tmp_path / "swhids.txt"
    swhids_file.write_text(
        "swh:1:cnt:0000000000000000000000000000000000000001\n"
        "swh:1:cnt:0000000000000000000000000000000000000005\n"  # skipped_content
        "swh:1:dir:0000000000000000000000000000000000000010\n"
        "swh:1:rev:0000000000000000000000000000000000000030\n"
        "swh:1:rel:0000000000000000000000000000000000000040\n"
        "swh:1:snp:0000000000000000000000000000000000000020\n"
        f"swh:1:ori:{data['url1_hash']}\n"
    )

    generate_subdataset_datafusion(dataset_path, output_path, swhids_file)

    # Verify all tables have correct output
    assert orc.read_table(str(output_path / "content" / "content.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "skipped_content" / "skipped_content.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "directory" / "directory.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "directory_entry" / "directory_entry.orc")).num_rows == 2
    assert orc.read_table(str(output_path / "revision" / "revision.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "revision_extra_headers" / "revision_extra_headers.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "release" / "release.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "snapshot" / "snapshot.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "snapshot_branch" / "snapshot_branch.orc")).num_rows == 2
    assert orc.read_table(str(output_path / "origin" / "origin.orc")).num_rows == 1
    assert orc.read_table(str(output_path / "origin_visit" / "origin_visit.orc")).num_rows == 2
    assert orc.read_table(str(output_path / "origin_visit_status" / "origin_visit_status.orc")).num_rows == 2
    # revision_history: rev 30 has no parents, so no rows expected
    assert not (output_path / "revision_history" / "revision_history.orc").exists()
