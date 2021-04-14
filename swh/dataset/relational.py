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
    ],
    "revision": [
        ("id", "string"),
        ("message", "binary"),
        ("author", "binary"),
        ("date", "timestamp"),
        ("date_offset", "smallint"),
        ("committer", "binary"),
        ("committer_date", "timestamp"),
        ("committer_offset", "smallint"),
        ("directory", "string"),
    ],
    "directory": [
        ("id", "string"),
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
