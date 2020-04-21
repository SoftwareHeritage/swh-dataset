# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import subprocess


class ZSTFile:
    def __init__(self, path, mode='r'):
        if mode not in ('r', 'rb', 'w', 'wb'):
            raise ValueError(f"ZSTFile mode {mode} is invalid.")
        self.path = path
        self.mode = mode

    def __enter__(self):
        is_text = not (self.mode in ('rb', 'wb'))
        writing = self.mode in ('w', 'wb')
        if writing:
            cmd = ['zstd', '-q', '-o', self.path]
        else:
            cmd = ['zstdcat', self.path]
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
