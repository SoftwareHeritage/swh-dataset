# Copyright (C) 2025-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import csv
import os
from pathlib import Path
import subprocess
import tempfile

from tqdm import tqdm

from .exporters.tabular import BaseWriter


def process_fullnames(writer: BaseWriter, dedup_dir: Path) -> None:
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

        with open(result_file.name) as f:
            reader = csv.reader(f)
            with writer:
                for row in tqdm(
                    reader, desc="Writing persons' fullnames to final file"
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
