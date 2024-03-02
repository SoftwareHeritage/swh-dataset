# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List, Union

# fmt: off
MAIN_TABLES = {
    "origin": [
        ("id", "string"),
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
    # snapshot_branches is in RELATED_TABLES
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
        ("committer_offset", "smallint"),  # called committer_date_offset in swh-storage
        ("committer_date_raw_offset_bytes", "binary"),
        ("directory", "string"),
        ("type", "string"),
        ("raw_manifest", "binary"),
    ],
    # revision_history is in RELATED_TABLES
    # revision_extra_headers is in RELATED_TABLES
    "directory": [
        ("id", "string"),
        ("raw_manifest", "binary"),
    ],
    # direcory_entry is in RELATED_TABLES
    "content": [
        ("sha1", "string"),
        ("sha1_git", "string"),
        ("sha256", "string"),
        ("blake2s256", "string"),
        ("length", "bigint"),
        ("status", "string"),
        ("data", "binary")
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

RELATION_TABLES = {
    "snapshot_branch": [
        ("snapshot_id", "string"),
        ("name", "binary"),
        ("target", "string"),
        ("target_type", "string"),
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
    "directory_entry": [
        ("directory_id", "string"),
        ("name", "binary"),
        ("type", "string"),
        ("target", "string"),
        ("perms", "int"),
    ],
}

TABLES = {**MAIN_TABLES, **RELATION_TABLES}

BLOOM_FILTER_COLUMNS: Dict[str, List[Union[str, int]]] = {
    "origin": [
        "url",  # allows checking if an origin is in the dataset
    ],
    "origin_visit": [
        "origin",  # allows listing visits of an origin
    ],
    "origin_visit_status": [
        "origin",  # allows listing visit statuses of an origin
    ],
    "snapshot": [
        "id",  # allows checking a snapshot is in the dataset
    ],
    "release": [
        "id",
        "author",
        "target",  # allows reverse traversal
    ],
    "revision": [
        "id",
        "author",
        "committer",
        "directory",  # allows reverse traversal
    ],
    "directory": [
        "id",  # allows checking a directory is in the dataset
    ],
    "content": [
        "sha1",
        "sha1_git",
        "sha256",
        # not including blake2s256, it's unlikely to be useful
    ],
    "skipped_content": [
        "sha1",
        "sha1_git",
        "sha256",
        # not including blake2s256, it's unlikely to be useful
    ],
    "snapshot_branch": [
        "snapshot_id",  # allows finding the stripe containing a snapshot's branches
        "target",  # allows reverse traversal
    ],
    "revision_history": [
        "id",  # allows finding the stripe containing a revision's parents
        "parent_id",  # allows reverse traversal
    ],
    "revision_extra_headers": [],
    "directory_entry": [
        "directory_id",  # allows finding the stripe containing a revision's parents
        "target",  # allows reverse traversal
        # not including name, it is often filtered with wildcards and/or case
        # normalization, so Bloom Filters cannot be used.
    ],
}
"""
Columns where we include Bloom filters.

They allow looking for high cardinality values without decompressing most stripes
not containing any (equality) match.
"""

# fmt: on
