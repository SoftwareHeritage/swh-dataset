# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import sqlite3
import subprocess

try:
    # Plyvel shouldn't be a hard dependency if we want to use sqlite instead
    import plyvel
except ImportError:
    plyvel = None


class ZSTFile:
    """
    Object-like wrapper around a ZST file. Uses a subprocess of the "zstd"
    command to compress and deflate the objects.
    """

    def __init__(self, path: str, mode: str = "r"):
        if mode not in ("r", "rb", "w", "wb"):
            raise ValueError(f"ZSTFile mode {mode} is invalid.")
        self.path = path
        self.mode = mode

    def __enter__(self) -> "ZSTFile":
        is_text = not (self.mode in ("rb", "wb"))
        writing = self.mode in ("w", "wb")
        if writing:
            cmd = ["zstd", "-f", "-q", "-o", self.path]
        else:
            cmd = ["zstdcat", self.path]
        self.process = subprocess.Popen(
            cmd,
            text=is_text,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.process.stdin.close()
        self.process.stdout.close()
        self.process.wait()

    def read(self, *args):
        return self.process.stdout.read(*args)

    def write(self, buf):
        self.process.stdin.write(buf)


class SQLiteSet:
    """
    On-disk Set object for hashes using SQLite as an indexer backend. Used to
    deduplicate objects when processing large queues with duplicates.
    """

    def __init__(self, db_path):
        self.db_path = db_path

    def __enter__(self):
        self.db = sqlite3.connect(str(self.db_path))
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS"
            " tmpset (val TEXT NOT NULL PRIMARY KEY)"
            " WITHOUT ROWID"
        )
        self.db.execute("PRAGMA synchronous = OFF")
        self.db.execute("PRAGMA journal_mode = OFF")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.commit()
        self.db.close()

    def add(self, v: bytes) -> bool:
        """
        Add an item to the set.

        Args:
            v: The value to add to the set.

        Returns:
              True if the value was added to the set, False if it was already present.
        """
        try:
            self.db.execute("INSERT INTO tmpset(val) VALUES (?)", (v.hex(),))
        except sqlite3.IntegrityError:
            return False
        else:
            return True


class LevelDBSet:
    """
    On-disk Set object for hashes using LevelDB as an indexer backend. Used to
    deduplicate objects when processing large queues with duplicates.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        if plyvel is None:
            raise ImportError("Plyvel library not found, required for LevelDBSet")

    def __enter__(self):
        self.db = plyvel.DB(str(self.db_path), create_if_missing=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def add(self, v: bytes) -> bool:
        """
        Add an item to the set.

        Args:
            v: The value to add to the set.

        Returns:
              True if the value was added to the set, False if it was already present.
        """
        if self.db.get(v):
            return False
        else:
            self.db.put(v, b"T")
            return True


def remove_pull_requests(snapshot):
    """
    Heuristic to filter out pull requests in snapshots: remove all branches
    that start with refs/ but do not start with refs/heads or refs/tags.
    """
    # Copy the items with list() to remove items during iteration
    for branch_name, branch in list(snapshot["branches"].items()):
        original_branch_name = branch_name
        while branch and branch.get("target_type") == "alias":
            branch_name = branch["target"]
            branch = snapshot["branches"].get(branch_name)
        if branch is None or not branch_name:
            continue
        if branch_name.startswith(b"refs/") and not (
            branch_name.startswith(b"refs/heads")
            or branch_name.startswith(b"refs/tags")
        ):
            snapshot["branches"].pop(original_branch_name)
