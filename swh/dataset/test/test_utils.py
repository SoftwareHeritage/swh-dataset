# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.dataset.utils import SQLiteSet


def test_sqliteset(tmp_path):
    f = tmp_path / "test.sqlite3"

    with SQLiteSet(f) as s:
        assert s.add(b"a")
        assert s.add(b"b")
        assert not s.add(b"a")
        assert s.add(b"c")
        assert not s.add(b"b")
        assert not s.add(b"c")
        assert not s.add(b"c")
