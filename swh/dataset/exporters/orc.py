# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import uuid

from pyorc import BigInt, Binary, Int, SmallInt, String, Struct, Timestamp, Writer

from swh.dataset.exporter import ExporterDispatch
from swh.dataset.relational import TABLES
from swh.dataset.utils import remove_pull_requests
from swh.model.hashutil import hash_to_hex

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


def swh_date_to_datetime(obj):
    if obj is None or obj["timestamp"] is None:
        return None
    return datetime.datetime(
        1970, 1, 1, tzinfo=datetime.timezone.utc
    ) + datetime.timedelta(
        seconds=obj["timestamp"]["seconds"],
        microseconds=obj["timestamp"]["microseconds"],
    )


def swh_date_to_offset(obj):
    if obj is None:
        return None
    return obj["offset"]


class ORCExporter(ExporterDispatch):
    """
    Implementation of an exporter which writes the entire graph dataset as
    ORC files. Useful for large scale processing, notably on cloud instances
    (e.g BigQuery, Amazon Athena, Azure).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writers = {}

    def get_writer_for(self, table_name: str):
        if table_name not in self.writers:
            object_type_dir = self.export_path / table_name
            object_type_dir.mkdir(exist_ok=True)
            unique_id = str(uuid.uuid4())
            export_file = object_type_dir / ("graph-{}.orc".format(unique_id))
            export_obj = self.exit_stack.enter_context(export_file.open("wb"))
            self.writers[table_name] = self.exit_stack.enter_context(
                Writer(export_obj, EXPORT_SCHEMA[table_name])
            )
        return self.writers[table_name]

    def process_origin(self, origin):
        origin_writer = self.get_writer_for("origin")
        origin_writer.write((origin["url"],))

    def process_origin_visit(self, visit):
        origin_visit_writer = self.get_writer_for("origin_visit")
        origin_visit_writer.write(
            (visit["origin"], visit["visit"], visit["date"], visit["type"],)
        )

    def process_origin_visit_status(self, visit_status):
        origin_visit_status_writer = self.get_writer_for("origin_visit_status")
        origin_visit_status_writer.write(
            (
                visit_status["origin"],
                visit_status["visit"],
                visit_status["date"],
                visit_status["status"],
                hash_to_hex_or_none(visit_status["snapshot"]),
            )
        )

    def process_snapshot(self, snapshot):
        if self.config.get("remove_pull_requests"):
            remove_pull_requests(snapshot)
        snapshot_writer = self.get_writer_for("snapshot")
        snapshot_writer.write((hash_to_hex_or_none(snapshot["id"]),))

        snapshot_branch_writer = self.get_writer_for("snapshot_branch")
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
                swh_date_to_datetime(release["date"]),
                swh_date_to_offset(release["date"]),
            )
        )

    def process_revision(self, revision):
        release_writer = self.get_writer_for("revision")
        release_writer.write(
            (
                hash_to_hex_or_none(revision["id"]),
                revision["message"],
                revision["author"]["fullname"],
                swh_date_to_datetime(revision["date"]),
                swh_date_to_offset(revision["date"]),
                revision["committer"]["fullname"],
                swh_date_to_datetime(revision["committer_date"]),
                swh_date_to_offset(revision["committer_date"]),
                hash_to_hex_or_none(revision["directory"]),
            )
        )

        revision_history_writer = self.get_writer_for("revision_history")
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

        directory_entry_writer = self.get_writer_for("directory_entry")
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
