# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import functools
import gc
import hashlib
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple

import pyorc

from swh.export.exporters import orc
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


def orc_load(rootdir: Path) -> Dict[str, Any]:
    res: Dict[str, Any] = collections.defaultdict(list)
    res["rootdir"] = rootdir
    for obj_type_dir in rootdir.iterdir():
        for orc_file in obj_type_dir.iterdir():
            with orc_file.open("rb") as orc_obj:
                reader = pyorc.Reader(
                    orc_obj,
                    converters={pyorc.TypeKind.TIMESTAMP: orc.SWHTimestampConverter},
                )
                obj_type = reader.user_metadata["swh_object_type"].decode()
                res[obj_type].extend(reader)
    return res


def assert_origins_exported_to_orc(
    origins: Sequence[model.Origin],
    orc_origins: Sequence[Tuple[Any, ...]],
):
    for ori in origins:
        sha1 = hashlib.sha1(ori.url.encode()).hexdigest()
        assert (sha1, ori.url) in orc_origins


def assert_origin_visits_exported_to_orc(
    origin_visits: Sequence[model.OriginVisit],
    orc_origin_visits: Sequence[Tuple[Any, ...]],
):
    assert len(origin_visits) == len(orc_origin_visits)
    for visit in origin_visits:
        assert (
            visit.origin,
            visit.visit,
            orc.datetime_to_tuple(visit.date),
            visit.type,
        ) in orc_origin_visits


def assert_origin_visit_statuses_exported_to_orc(
    origin_visit_statuses: Sequence[model.OriginVisitStatus],
    orc_origin_visit_statuses: Sequence[Tuple[Any, ...]],
):
    for visit_status in origin_visit_statuses:
        assert (
            visit_status.origin,
            visit_status.visit,
            orc.datetime_to_tuple(visit_status.date),
            visit_status.status,
            orc.hash_to_hex_or_none(visit_status.snapshot),
            visit_status.type,
        ) in orc_origin_visit_statuses


def assert_snapshots_exported_to_orc(
    snapshots: Sequence[model.Snapshot],
    orc_snapshots: Sequence[Tuple[Any, ...]],
    orc_snapshot_branches: Sequence[Tuple[Any, ...]],
):
    for snp in snapshots:
        assert (orc.hash_to_hex_or_none(snp.id),) in orc_snapshots
        for branch_name, branch in snp.branches.items():
            if branch is None:
                continue
            assert (
                orc.hash_to_hex_or_none(snp.id),
                branch_name,
                orc.hash_to_hex_or_none(branch.target),
                str(branch.target_type.value),
            ) in orc_snapshot_branches


def assert_releases_exported_to_orc(
    releases: Sequence[model.Release],
    orc_releases: Sequence[Tuple[Any, ...]],
):
    for rel in releases:
        assert (
            orc.hash_to_hex_or_none(rel.id),
            rel.name,
            rel.message,
            orc.hash_to_hex_or_none(rel.target),
            rel.target_type.value,
            rel.author.fullname if rel.author else None,
            *orc.swh_date_to_tuple(getattr(rel, "date", None)),
            rel.raw_manifest,
        ) in orc_releases


def assert_revisions_exported_to_orc(
    revisions: Sequence[model.Revision],
    orc_revisions: Sequence[Tuple[Any, ...]],
    orc_revisions_history: Sequence[Tuple[Any, ...]],
):
    for rev in revisions:
        assert (
            orc.hash_to_hex_or_none(rev.id),
            rev.message,
            rev.author.fullname if rev.author else None,
            *orc.swh_date_to_tuple(getattr(rev, "date", None)),
            rev.committer.fullname if rev.committer else None,
            *orc.swh_date_to_tuple(getattr(rev, "committer_date", None)),
            orc.hash_to_hex_or_none(rev.directory),
            rev.type.value,
            rev.raw_manifest,
        ) in orc_revisions
        for i, parent in enumerate(rev.parents):
            assert (
                orc.hash_to_hex_or_none(rev.id),
                orc.hash_to_hex_or_none(parent),
                i,
            ) in orc_revisions_history


def assert_directories_exported_to_orc(
    directories: Sequence[model.Directory],
    orc_directories: Sequence[Tuple[Any, ...]],
    orc_directories_entries: Sequence[Tuple[Any, ...]],
):
    for dir_ in directories:
        assert (orc.hash_to_hex_or_none(dir_.id), dir_.raw_manifest) in orc_directories
        for entry in dir_.entries:
            assert (
                orc.hash_to_hex_or_none(dir_.id),
                entry.name,
                entry.type,
                orc.hash_to_hex_or_none(entry.target),
                entry.perms,
            ) in orc_directories_entries


def assert_contents_exported_to_orc(
    contents: Sequence[model.Content],
    orc_contents: Sequence[Tuple[Any, ...]],
):
    for cnt in contents:
        assert (
            orc.hash_to_hex_or_none(cnt.sha1),
            orc.hash_to_hex_or_none(cnt.sha1_git),
            orc.hash_to_hex_or_none(cnt.sha256),
            orc.hash_to_hex_or_none(cnt.blake2s256),
            cnt.length,
            cnt.status,
            None,
        ) in orc_contents


def assert_skipped_contents_exported_to_orc(
    skipped_contents: Sequence[model.SkippedContent],
    orc_skipped_contents: Sequence[Tuple[Any, ...]],
):
    for cnt in skipped_contents:
        assert (
            orc.hash_to_hex_or_none(cnt.sha1),
            orc.hash_to_hex_or_none(cnt.sha1_git),
            orc.hash_to_hex_or_none(cnt.sha256),
            orc.hash_to_hex_or_none(cnt.blake2s256),
            cnt.length,
            cnt.status,
            cnt.reason,
        ) in orc_skipped_contents
