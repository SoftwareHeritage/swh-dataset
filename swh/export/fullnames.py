# Copyright (C) 2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import csv
import os
from pathlib import Path
import subprocess
import tempfile

import pyorc
from tqdm import tqdm


def process_fullnames(fullnames_orc: Path, dedup_dir: Path) -> None:
    with tempfile.NamedTemporaryFile(suffix=".csv") as result_file:
        entries = list(dedup_dir.iterdir())
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
