# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import datetime
import functools
import gc
import hashlib
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple

import pyarrow
import pyorc

from swh.export.exporters import orc, tabular
from swh.export.exporters.tabular import datetime_to_tuple, hash_to_hex_or_none
from swh.model import model


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


def load(exporter, rootdir: Path) -> Dict[str, Any]:
    res: Dict[str, Any] = collections.defaultdict(list)
    res["rootdir"] = rootdir
    for obj_type_dir in rootdir.iterdir():
        for path in obj_type_dir.iterdir():
            with path.open("rb") as file:
                if exporter == "orc":
                    orc_reader = pyorc.Reader(
                        file,
                        converters={
                            pyorc.TypeKind.TIMESTAMP: orc.SWHTimestampConverter
                        },
                    )
                    obj_type = orc_reader.user_metadata["swh_object_type"].decode()
                    res[obj_type].extend(orc_reader)
                else:
                    assert exporter == "parquet"
                    parquet_reader = pyarrow.parquet.ParquetFile(path)
                    metadata = parquet_reader.metadata.metadata
                    assert metadata is not None
                    obj_type = metadata["swh_object_type".encode()].decode()

                    def to_tuple(d):
                        """convert Parquet's dicts to ORC-like tuples"""
                        if isinstance(d, dict):
                            return tuple(map(to_tuple, d.values()))
                        elif isinstance(d, datetime.datetime):
                            return datetime_to_tuple(d)
                        else:
                            return d

                    res[obj_type].extend(
                        to_tuple(row) for row in parquet_reader.read().to_pylist()
                    )
    return res


def assert_origins_exported(
    origins: Sequence[model.Origin],
    orc_origins: Sequence[Tuple[Any, ...]],
):
    for ori in origins:
        sha1 = hashlib.sha1(ori.url.encode()).hexdigest()
        assert (sha1, ori.url) in orc_origins


def assert_origin_visits_exported(
    origin_visits: Sequence[model.OriginVisit],
    orc_origin_visits: Sequence[Tuple[Any, ...]],
):
    assert len(origin_visits) == len(orc_origin_visits)
    for visit in origin_visits:
        assert (
            visit.origin,
            visit.visit,
            tabular.datetime_to_tuple(visit.date),
            visit.type,
        ) in orc_origin_visits


def assert_origin_visit_statuses_exported(
    origin_visit_statuses: Sequence[model.OriginVisitStatus],
    orc_origin_visit_statuses: Sequence[Tuple[Any, ...]],
):
    for visit_status in origin_visit_statuses:
        assert (
            visit_status.origin,
            visit_status.visit,
            tabular.datetime_to_tuple(visit_status.date),
            visit_status.status,
            hash_to_hex_or_none(visit_status.snapshot),
            visit_status.type,
        ) in orc_origin_visit_statuses


def assert_snapshots_exported(
    snapshots: Sequence[model.Snapshot],
    orc_snapshots: Sequence[Tuple[Any, ...]],
    orc_snapshot_branches: Sequence[Tuple[Any, ...]],
):
    for snp in snapshots:
        assert (hash_to_hex_or_none(snp.id),) in orc_snapshots
        for branch_name, branch in snp.branches.items():
            if branch is None:
                continue
            assert (
                hash_to_hex_or_none(snp.id),
                branch_name,
                hash_to_hex_or_none(branch.target),
                str(branch.target_type.value),
            ) in orc_snapshot_branches


def assert_releases_exported(
    releases: Sequence[model.Release],
    orc_releases: Sequence[Tuple[Any, ...]],
):
    for rel in releases:
        assert (
            hash_to_hex_or_none(rel.id),
            rel.name,
            rel.message,
            hash_to_hex_or_none(rel.target),
            rel.target_type.value,
            rel.author.fullname if rel.author else None,
            *tabular.swh_date_to_tuple(getattr(rel, "date", None)),
            rel.raw_manifest,
        ) in orc_releases


def assert_revisions_exported(
    revisions: Sequence[model.Revision],
    orc_revisions: Sequence[Tuple[Any, ...]],
    orc_revisions_history: Sequence[Tuple[Any, ...]],
):
    for rev in revisions:
        assert (
            hash_to_hex_or_none(rev.id),
            rev.message,
            rev.author.fullname if rev.author else None,
            *tabular.swh_date_to_tuple(getattr(rev, "date", None)),
            rev.committer.fullname if rev.committer else None,
            *tabular.swh_date_to_tuple(getattr(rev, "committer_date", None)),
            hash_to_hex_or_none(rev.directory),
            rev.type.value,
            rev.raw_manifest,
        ) in orc_revisions
        for i, parent in enumerate(rev.parents):
            assert (
                hash_to_hex_or_none(rev.id),
                hash_to_hex_or_none(parent),
                i,
            ) in orc_revisions_history


def assert_directories_exported(
    directories: Sequence[model.Directory],
    orc_directories: Sequence[Tuple[Any, ...]],
    orc_directories_entries: Sequence[Tuple[Any, ...]],
):
    for dir_ in directories:
        assert (hash_to_hex_or_none(dir_.id), dir_.raw_manifest) in orc_directories
        for entry in dir_.entries:
            assert (
                hash_to_hex_or_none(dir_.id),
                entry.name,
                entry.type,
                hash_to_hex_or_none(entry.target),
                entry.perms,
            ) in orc_directories_entries


def assert_contents_exported(
    contents: Sequence[model.Content],
    orc_contents: Sequence[Tuple[Any, ...]],
):
    for cnt in contents:
        assert (
            hash_to_hex_or_none(cnt.sha1),
            hash_to_hex_or_none(cnt.sha1_git),
            hash_to_hex_or_none(cnt.sha256),
            hash_to_hex_or_none(cnt.blake2s256),
            cnt.length,
            cnt.status,
            None,
        ) in orc_contents


def assert_skipped_contents_exported(
    skipped_contents: Sequence[model.SkippedContent],
    orc_skipped_contents: Sequence[Tuple[Any, ...]],
):
    for cnt in skipped_contents:
        assert (
            hash_to_hex_or_none(cnt.sha1),
            hash_to_hex_or_none(cnt.sha1_git),
            hash_to_hex_or_none(cnt.sha256),
            hash_to_hex_or_none(cnt.blake2s256),
            cnt.length,
            cnt.status,
            cnt.reason,
        ) in orc_skipped_contents
