# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# fmt: off
TABLES = {
    "origin": [
        ("url", "string"),
    ],
    "origin_visit": [
        ("origin", "string"),
        ("visit", "bigint"),
        ("date", "timestamp"),
        ("type", "string"),
    ],
    "origin_visit_status": [
        ("origin", "string"),
        ("visit", "bigint"),
        ("date", "timestamp"),
        ("status", "string"),
        ("snapshot", "string"),
        ("type", "string"),
    ],
    "snapshot": [
        ("id", "string"),
    ],
    "snapshot_branch": [
        ("snapshot_id", "string"),
        ("name", "binary"),
        ("target", "string"),
        ("target_type", "string"),
    ],
    "release": [
        ("id", "string"),
        ("name", "binary"),
        ("message", "binary"),
        ("target", "string"),
        ("target_type", "string"),
        ("author", "binary"),
        ("date", "timestamp"),
        ("date_offset", "smallint"),
        ("date_raw_offset_bytes", "binary"),
        ("raw_manifest", "binary"),
    ],
    "revision": [
        ("id", "string"),
        ("message", "binary"),
        ("author", "binary"),
        ("date", "timestamp"),
        ("date_offset", "smallint"),
        ("date_raw_offset_bytes", "binary"),
        ("committer", "binary"),
        ("committer_date", "timestamp"),
        ("committer_offset", "smallint"),
        ("committer_date_raw_offset_bytes", "binary"),
        ("directory", "string"),
        ("type", "string"),
        ("raw_manifest", "binary"),
    ],
    "revision_history": [
        ("id", "string"),
        ("parent_id", "string"),
        ("parent_rank", "int"),
    ],
    "revision_extra_headers": [
        ("id", "string"),
        ("key", "binary"),
        ("value", "binary"),
    ],
    "directory": [
        ("id", "string"),
        ("raw_manifest", "binary"),
    ],
    "directory_entry": [
        ("directory_id", "string"),
        ("name", "binary"),
        ("type", "string"),
        ("target", "string"),
        ("perms", "int"),
    ],
    "content": [
        ("sha1", "string"),
        ("sha1_git", "string"),
        ("sha256", "string"),
        ("blake2s256", "string"),
        ("length", "bigint"),
        ("status", "string"),
    ],
    "skipped_content": [
        ("sha1", "string"),
        ("sha1_git", "string"),
        ("sha256", "string"),
        ("blake2s256", "string"),
        ("length", "bigint"),
        ("status", "string"),
        ("reason", "string"),
    ],
}
# fmt: on
