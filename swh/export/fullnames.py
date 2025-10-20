# Copyright (C) 2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import contextlib
import csv
import hashlib
import logging
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Optional

import pyorc
from tqdm import tqdm

from swh.model.model import Person

FULLNAME_SIZE_LIMIT = 32000
"""Exclude fullnames bigger than this number of bytes."""

logger = logging.getLogger(__name__)


class FullnameWriter(contextlib.AbstractContextManager):
    def __init__(self, output_path: Path):
        self._output_path = output_path

    def __enter__(self):
        self.persons_sorter = subprocess.Popen(
            # fmt: off
            [
                "sort",
                "-t", ",",
                "-k", "2",
                "-S", "100M",
                "-u",
                "-o",
                self._output_path,
            ],
            # fmt: on
            env={**os.environ, "LC_ALL": "C", "LC_COLLATE": "C", "LANG": "C"},
            universal_newlines=True,
            stdin=subprocess.PIPE,
        )
        assert self.persons_sorter.stdin is not None
        self.persons_writer = csv.writer(self.persons_sorter.stdin)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.persons_sorter.stdin.close()
        logger.debug("Starting persons partial deduplication")
        self.persons_sorter.wait()
        logger.debug("Persons partial deduplication done")

    def truncate_person(self, person: Person) -> Optional[Person]:
        if len(person.fullname) > FULLNAME_SIZE_LIMIT:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"{person.fullname.decode(errors='replace')} is too long, truncating"
                )
            return Person(
                fullname=person.fullname[:FULLNAME_SIZE_LIMIT], name=None, email=None
            )
        return None

    def add_person(self, person: Person) -> None:
        self.persons_writer.writerow(
            (
                base64.b64encode(person.fullname).decode(),
                base64.b64encode((hashlib.sha256(person.fullname)).digest()).decode(),
            )
        )


class ParallelFullnameWriter:
    def __init__(self):
        self.dedup_dir = tempfile.TemporaryDirectory()

    def write_to_orc(self, fullnames_orc: Path) -> None:
        with tempfile.NamedTemporaryFile(suffix=".csv") as result_file:
            entries = list(Path(self.dedup_dir.name).iterdir())
            if entries:
                env = {**os.environ, "LC_ALL": "C", "LC_COLLATE": "C", "LANG": "C"}
                # fmt: off
                subprocess.run(
                    [
                        "sort",
                        "-t", ",",
                        "-k", "2",
                        "-u",
                        "-S", "100M",
                        "-m",
                        *entries,
                        "-o", result_file.name,
                    ],
                    env=env,
                )
                # fmt: on

            with open(fullnames_orc, "wb") as output:
                with open(result_file.name, "r") as input:
                    reader = csv.reader(input)
                    with pyorc.Writer(
                        output,
                        pyorc.Struct(
                            fullname=pyorc.Binary(), sha256_fullname=pyorc.Binary()
                        ),
                        bloom_filter_columns=[0, 1],
                    ) as writer:
                        for row in tqdm(
                            reader, desc="Writing persons' fullnames to ORC file"
                        ):
                            if row == ("",):
                                continue
                            fullname, sha256_fullname = row
                            writer.write(
                                (
                                    base64.b64decode(fullname),
                                    base64.b64decode(sha256_fullname),
                                )
                            )

    def get_thread_writer(self) -> FullnameWriter:
        (_fd, filename) = tempfile.mkstemp(dir=self.dedup_dir.name, suffix=".csv")
        return FullnameWriter(Path(filename))
