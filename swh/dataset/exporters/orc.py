# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
import math
from typing import Any, Optional, Tuple, Type, cast

from pkg_resources import get_distribution
from pyorc import (
    BigInt,
    Binary,
    CompressionKind,
    Int,
    SmallInt,
    String,
    Struct,
    Timestamp,
    TypeKind,
    Writer,
)
from pyorc.converters import ORCConverter

from swh.dataset.exporter import ExporterDispatch
from swh.dataset.relational import TABLES
from swh.dataset.utils import remove_pull_requests
from swh.model.hashutil import hash_to_hex
from swh.model.model import TimestampWithTimezone

ORC_TYPE_MAP = {
    "string": String,
    "smallint": SmallInt,
    "int": Int,
    "bigint": BigInt,
    "timestamp": Timestamp,
    "binary": Binary,
}

EXPORT_SCHEMA = {
    table_name: Struct(
        **{
            column_name: ORC_TYPE_MAP[column_type]()
            for column_name, column_type in columns
        }
    )
    for table_name, columns in TABLES.items()
}


def hash_to_hex_or_none(hash):
    return hash_to_hex(hash) if hash is not None else None


def swh_date_to_tuple(obj):
    if obj is None or obj["timestamp"] is None:
        return (None, None, None)
    offset_bytes = obj.get("offset_bytes")
    if offset_bytes is None:
        offset = obj.get("offset", 0)
        negative = offset < 0 or obj.get("negative_utc", False)
        (hours, minutes) = divmod(abs(offset), 60)
        offset_bytes = f"{'-' if negative else '+'}{hours:02}{minutes:02}".encode()
    else:
        offset = TimestampWithTimezone._parse_offset_bytes(offset_bytes)
    return (
        (obj["timestamp"]["seconds"], obj["timestamp"]["microseconds"]),
        offset,
        offset_bytes,
    )


def datetime_to_tuple(obj: Optional[datetime]) -> Optional[Tuple[int, int]]:
    if obj is None:
        return None
    return (math.floor(obj.timestamp()), obj.microsecond)


class SWHTimestampConverter:
    """This is an ORCConverter compatible class to convert timestamps from/to ORC files

    timestamps in python are given as a couple (seconds, microseconds) and are
    serialized as a couple (seconds, nanoseconds) in the ORC file.

    Reimplemented because we do not want the Python object to be converted as
    ORC timestamp to be Python datatime objects, since swh.model's Timestamp
    cannot be converted without loss a Python datetime objects.
    """

    # use Any as timezone annotation to make it easier to run mypy on python <
    # 3.9, plus we do not use the timezone argument here...
    @staticmethod
    def from_orc(seconds: int, nanoseconds: int, timezone: Any,) -> Tuple[int, int]:
        return (seconds, nanoseconds // 1000)

    @staticmethod
    def to_orc(
        obj: Optional[Tuple[int, int]], timezone: Any,
    ) -> Optional[Tuple[int, int]]:
        if obj is None:
            return None
        return (obj[0], obj[1] * 1000 if obj[1] is not None else None)


class ORCExporter(ExporterDispatch):
    """
    Implementation of an exporter which writes the entire graph dataset as
    ORC files. Useful for large scale processing, notably on cloud instances
    (e.g BigQuery, Amazon Athena, Azure).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writers = {}
        self.uuids = {}

    def get_writer_for(self, table_name: str, directory_name=None, unique_id=None):
        if table_name not in self.writers:
            if directory_name is None:
                directory_name = table_name
            object_type_dir = self.export_path / directory_name
            object_type_dir.mkdir(exist_ok=True)
            if unique_id is None:
                unique_id = unique_id = self.get_unique_file_id()
            export_file = object_type_dir / (f"{table_name}-{unique_id}.orc")
            export_obj = self.exit_stack.enter_context(export_file.open("wb"))
            self.writers[table_name] = self.exit_stack.enter_context(
                Writer(
                    export_obj,
                    EXPORT_SCHEMA[table_name],
                    compression=CompressionKind.ZSTD,
                    converters={
                        TypeKind.TIMESTAMP: cast(
                            Type[ORCConverter], SWHTimestampConverter
                        )
                    },
                )
            )
            self.writers[table_name].set_user_metadata(
                swh_object_type=table_name.encode(),
                swh_uuid=unique_id.encode(),
                swh_model_version=get_distribution("swh.model").version.encode(),
                swh_dataset_version=get_distribution("swh.dataset").version.encode(),
                # maybe put a copy of the config (redacted) also?
            )
            self.uuids[table_name] = unique_id

        return self.writers[table_name]

    def process_origin(self, origin):
        origin_writer = self.get_writer_for("origin")
        origin_writer.write((origin["url"],))

    def process_origin_visit(self, visit):
        origin_visit_writer = self.get_writer_for("origin_visit")
        origin_visit_writer.write(
            (
                visit["origin"],
                visit["visit"],
                datetime_to_tuple(visit["date"]),
                visit["type"],
            )
        )

    def process_origin_visit_status(self, visit_status):
        origin_visit_status_writer = self.get_writer_for("origin_visit_status")
        origin_visit_status_writer.write(
            (
                visit_status["origin"],
                visit_status["visit"],
                datetime_to_tuple(visit_status["date"]),
                visit_status["status"],
                hash_to_hex_or_none(visit_status["snapshot"]),
            )
        )

    def process_snapshot(self, snapshot):
        if self.config.get("remove_pull_requests"):
            remove_pull_requests(snapshot)
        snapshot_writer = self.get_writer_for("snapshot")
        snapshot_writer.write((hash_to_hex_or_none(snapshot["id"]),))

        # we want to store branches in the same directory as snapshot objects,
        # and have both files have the same UUID.
        snapshot_branch_writer = self.get_writer_for(
            "snapshot_branch",
            directory_name="snapshot",
            unique_id=self.uuids["snapshot"],
        )
        for branch_name, branch in snapshot["branches"].items():
            if branch is None:
                continue
            snapshot_branch_writer.write(
                (
                    hash_to_hex_or_none(snapshot["id"]),
                    branch_name,
                    hash_to_hex_or_none(branch["target"]),
                    branch["target_type"],
                )
            )

    def process_release(self, release):
        release_writer = self.get_writer_for("release")
        release_writer.write(
            (
                hash_to_hex_or_none(release["id"]),
                release["name"],
                release["message"],
                hash_to_hex_or_none(release["target"]),
                release["target_type"],
                (release.get("author") or {}).get("fullname"),
                *swh_date_to_tuple(release["date"]),
            )
        )

    def process_revision(self, revision):
        release_writer = self.get_writer_for("revision")
        release_writer.write(
            (
                hash_to_hex_or_none(revision["id"]),
                revision["message"],
                revision["author"]["fullname"],
                *swh_date_to_tuple(revision["date"]),
                revision["committer"]["fullname"],
                *swh_date_to_tuple(revision["committer_date"]),
                hash_to_hex_or_none(revision["directory"]),
            )
        )

        revision_history_writer = self.get_writer_for(
            "revision_history",
            directory_name="revision",
            unique_id=self.uuids["revision"],
        )
        for i, parent_id in enumerate(revision["parents"]):
            revision_history_writer.write(
                (
                    hash_to_hex_or_none(revision["id"]),
                    hash_to_hex_or_none(parent_id),
                    i,
                )
            )

    def process_directory(self, directory):
        directory_writer = self.get_writer_for("directory")
        directory_writer.write((hash_to_hex_or_none(directory["id"]),))

        directory_entry_writer = self.get_writer_for(
            "directory_entry",
            directory_name="directory",
            unique_id=self.uuids["directory"],
        )
        for entry in directory["entries"]:
            directory_entry_writer.write(
                (
                    hash_to_hex_or_none(directory["id"]),
                    entry["name"],
                    entry["type"],
                    hash_to_hex_or_none(entry["target"]),
                    entry["perms"],
                )
            )

    def process_content(self, content):
        content_writer = self.get_writer_for("content")
        content_writer.write(
            (
                hash_to_hex_or_none(content["sha1"]),
                hash_to_hex_or_none(content["sha1_git"]),
                hash_to_hex_or_none(content["sha256"]),
                hash_to_hex_or_none(content["blake2s256"]),
                content["length"],
                content["status"],
            )
        )

    def process_skipped_content(self, skipped_content):
        skipped_content_writer = self.get_writer_for("skipped_content")
        skipped_content_writer.write(
            (
                hash_to_hex_or_none(skipped_content["sha1"]),
                hash_to_hex_or_none(skipped_content["sha1_git"]),
                hash_to_hex_or_none(skipped_content["sha256"]),
                hash_to_hex_or_none(skipped_content["blake2s256"]),
                skipped_content["length"],
                skipped_content["status"],
                skipped_content["reason"],
            )
        )
