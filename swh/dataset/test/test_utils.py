# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.dataset.utils import LevelDBSet, SQLiteSet


@pytest.fixture(params=[SQLiteSet, LevelDBSet])
def diskset(request, tmp_path):
    backend = request.param
    return backend(tmp_path / "test")


def test_diskset(diskset):
    with diskset as s:
        assert s.add(b"a")
        assert s.add(b"b")
        assert not s.add(b"a")
        assert s.add(b"c")
        assert not s.add(b"b")
        assert not s.add(b"c")
        assert not s.add(b"c")
