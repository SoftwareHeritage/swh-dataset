# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import sqlite3
import subprocess


class ZSTFile:
    """
    Object-like wrapper around a ZST file. Uses a subprocess of the "zstd"
    command to compress and deflate the objects.
    """

    def __init__(self, path, mode="r"):
        if mode not in ("r", "rb", "w", "wb"):
            raise ValueError(f"ZSTFile mode {mode} is invalid.")
        self.path = path
        self.mode = mode

    def __enter__(self):
        is_text = not (self.mode in ("rb", "wb"))
        writing = self.mode in ("w", "wb")
        if writing:
            cmd = ["zstd", "-q", "-o", self.path]
        else:
            cmd = ["zstdcat", self.path]
        self.process = subprocess.Popen(
            cmd, text=is_text, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
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

    def __init__(self, db_path: os.PathLike):
        self.db_path = db_path

    def __enter__(self):
        self.db = sqlite3.connect(str(self.db_path))
        self.db.execute(
            "CREATE TABLE tmpset (val TEXT NOT NULL PRIMARY KEY) WITHOUT ROWID"
        )
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
        try:
            self.db.execute("INSERT INTO tmpset(val) VALUES (?)", (v.hex(),))
        except sqlite3.IntegrityError:
            return False
        else:
            return True
