# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from base64 import b64encode
import collections
import hashlib
from typing import Tuple
from unittest.mock import Mock, call

import pytest

from swh.dataset.exporters.edges import GraphEdgesExporter, sort_graph_nodes
from swh.dataset.utils import ZSTFile
from swh.model.hashutil import MultiHash, hash_to_bytes

DATE = {
    "timestamp": {"seconds": 1234567891, "microseconds": 0},
    "offset": 120,
    "negative_utc": False,
}

TEST_CONTENT = {
    **MultiHash.from_data(b"foo").digest(),
    "length": 3,
    "status": "visible",
}

TEST_REVISION = {
    "id": hash_to_bytes("7026b7c1a2af56521e951c01ed20f255fa054238"),
    "message": b"hello",
    "date": DATE,
    "committer": {"fullname": b"foo", "name": b"foo", "email": b""},
    "author": {"fullname": b"foo", "name": b"foo", "email": b""},
    "committer_date": DATE,
    "type": "git",
    "directory": b"\x01" * 20,
    "synthetic": False,
    "metadata": None,
    "parents": [],
}

TEST_RELEASE = {
    "id": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    "name": b"v0.0.1",
    "date": {
        "timestamp": {"seconds": 1234567890, "microseconds": 0},
        "offset": 120,
        "negative_utc": False,
    },
    "author": {"author": {"fullname": b"foo", "name": b"foo", "email": b""}},
    "target_type": "revision",
    "target": b"\x04" * 20,
    "message": b"foo",
    "synthetic": False,
}

TEST_ORIGIN = {"url": "https://somewhere.org/den/fox"}
TEST_ORIGIN_2 = {"url": "https://somewhere.org/den/fox/2"}

TEST_ORIGIN_VISIT_STATUS = {
    "origin": TEST_ORIGIN["url"],
    "visit": 1,
    "date": "2013-05-07 04:20:39.369271+00:00",
    "snapshot": None,  # TODO
    "status": "ongoing",  # TODO
    "metadata": {"foo": "bar"},
}


class FakeDiskSet(set):
    """
    A set with an add() method that returns whether the item has been added
    or was already there. Used to replace disk sets in unittests.
    """

    def add(self, v):
        assert isinstance(v, bytes)
        r = True
        if v in self:
            r = False
        super().add(v)
        return r


@pytest.fixture
def exporter():
    def wrapped(messages, config=None) -> Tuple[Mock, Mock]:
        if config is None:
            config = {}
        exporter = GraphEdgesExporter(config, "/dummy_path")
        node_writer = Mock()
        edge_writer = Mock()
        exporter.get_writers_for = lambda *a, **k: (  # type: ignore
            node_writer,
            edge_writer,
        )
        for object_type, objects in messages.items():
            for obj in objects:
                exporter.process_object(object_type, obj)
        return node_writer.write, edge_writer.write

    return wrapped


def binhash(s):
    return hashlib.sha1(s.encode()).digest()


def hexhash(s):
    return hashlib.sha1(s.encode()).hexdigest()


def b64e(s: str) -> str:
    return b64encode(s.encode()).decode()


def test_export_origin(exporter):
    node_writer, edge_writer = exporter(
        {
            "origin": [
                {"url": "ori1"},
                {"url": "ori2"},
            ]
        }
    )
    assert node_writer.mock_calls == [
        call(f"swh:1:ori:{hexhash('ori1')}\n"),
        call(f"swh:1:ori:{hexhash('ori2')}\n"),
    ]
    assert edge_writer.mock_calls == []


def test_export_origin_visit_status(exporter):
    node_writer, edge_writer = exporter(
        {
            "origin_visit_status": [
                {
                    **TEST_ORIGIN_VISIT_STATUS,
                    "origin": "ori1",
                    "snapshot": binhash("snp1"),
                },
                {
                    **TEST_ORIGIN_VISIT_STATUS,
                    "origin": "ori2",
                    "snapshot": binhash("snp2"),
                },
            ]
        }
    )
    assert node_writer.mock_calls == []
    assert edge_writer.mock_calls == [
        call(f"swh:1:ori:{hexhash('ori1')} swh:1:snp:{hexhash('snp1')}\n"),
        call(f"swh:1:ori:{hexhash('ori2')} swh:1:snp:{hexhash('snp2')}\n"),
    ]


def test_export_snapshot_simple(exporter):
    node_writer, edge_writer = exporter(
        {
            "snapshot": [
                {
                    "id": binhash("snp1"),
                    "branches": {
                        b"refs/heads/master": {
                            "target": binhash("rev1"),
                            "target_type": "revision",
                        },
                        b"HEAD": {"target": binhash("rev1"), "target_type": "revision"},
                    },
                },
                {
                    "id": binhash("snp2"),
                    "branches": {
                        b"refs/heads/master": {
                            "target": binhash("rev1"),
                            "target_type": "revision",
                        },
                        b"HEAD": {"target": binhash("rev2"), "target_type": "revision"},
                        b"bcnt": {"target": binhash("cnt1"), "target_type": "content"},
                        b"bdir": {
                            "target": binhash("dir1"),
                            "target_type": "directory",
                        },
                        b"brel": {"target": binhash("rel1"), "target_type": "release"},
                        b"bsnp": {"target": binhash("snp1"), "target_type": "snapshot"},
                    },
                },
                {"id": binhash("snp3"), "branches": {}},
            ]
        }
    )
    assert node_writer.mock_calls == [
        call(f"swh:1:snp:{hexhash('snp1')}\n"),
        call(f"swh:1:snp:{hexhash('snp2')}\n"),
        call(f"swh:1:snp:{hexhash('snp3')}\n"),
    ]
    assert edge_writer.mock_calls == [
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('refs/heads/master')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('HEAD')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp2')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('refs/heads/master')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp2')} swh:1:rev:{hexhash('rev2')}"
            f" {b64e('HEAD')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp2')} swh:1:cnt:{hexhash('cnt1')}"
            f" {b64e('bcnt')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp2')} swh:1:dir:{hexhash('dir1')}"
            f" {b64e('bdir')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp2')} swh:1:rel:{hexhash('rel1')}"
            f" {b64e('brel')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp2')} swh:1:snp:{hexhash('snp1')}"
            f" {b64e('bsnp')}\n"
        ),
    ]


def test_export_snapshot_aliases(exporter):
    node_writer, edge_writer = exporter(
        {
            "snapshot": [
                {
                    "id": binhash("snp1"),
                    "branches": {
                        b"origin_branch": {
                            "target": binhash("rev1"),
                            "target_type": "revision",
                        },
                        b"alias1": {"target": b"origin_branch", "target_type": "alias"},
                        b"alias2": {"target": b"alias1", "target_type": "alias"},
                        b"alias3": {"target": b"alias2", "target_type": "alias"},
                    },
                },
            ]
        }
    )
    assert node_writer.mock_calls == [call(f"swh:1:snp:{hexhash('snp1')}\n")]
    assert edge_writer.mock_calls == [
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('origin_branch')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('alias1')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('alias2')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('alias3')}\n"
        ),
    ]


def test_export_snapshot_no_pull_requests(exporter):
    snp = {
        "id": binhash("snp1"),
        "branches": {
            b"refs/heads/master": {
                "target": binhash("rev1"),
                "target_type": "revision",
            },
            b"refs/pull/42": {"target": binhash("rev2"), "target_type": "revision"},
            b"refs/merge-requests/lol": {
                "target": binhash("rev3"),
                "target_type": "revision",
            },
            b"refs/tags/v1.0.0": {
                "target": binhash("rev4"),
                "target_type": "revision",
            },
            b"refs/patch/123456abc": {
                "target": binhash("rev5"),
                "target_type": "revision",
            },
        },
    }

    node_writer, edge_writer = exporter({"snapshot": [snp]})
    assert edge_writer.mock_calls == [
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('refs/heads/master')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev2')}"
            f" {b64e('refs/pull/42')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev3')}"
            f" {b64e('refs/merge-requests/lol')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev4')}"
            f" {b64e('refs/tags/v1.0.0')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev5')}"
            f" {b64e('refs/patch/123456abc')}\n"
        ),
    ]

    node_writer, edge_writer = exporter(
        {"snapshot": [snp]}, config={"remove_pull_requests": True}
    )
    assert edge_writer.mock_calls == [
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('refs/heads/master')}\n"
        ),
        call(
            f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev4')}"
            f" {b64e('refs/tags/v1.0.0')}\n"
        ),
    ]


def test_export_releases(exporter):
    node_writer, edge_writer = exporter(
        {
            "release": [
                {
                    **TEST_RELEASE,
                    "id": binhash("rel1"),
                    "target": binhash("rev1"),
                    "target_type": "revision",
                },
                {
                    **TEST_RELEASE,
                    "id": binhash("rel2"),
                    "target": binhash("rel1"),
                    "target_type": "release",
                },
                {
                    **TEST_RELEASE,
                    "id": binhash("rel3"),
                    "target": binhash("dir1"),
                    "target_type": "directory",
                },
                {
                    **TEST_RELEASE,
                    "id": binhash("rel4"),
                    "target": binhash("cnt1"),
                    "target_type": "content",
                },
            ]
        }
    )
    assert node_writer.mock_calls == [
        call(f"swh:1:rel:{hexhash('rel1')}\n"),
        call(f"swh:1:rel:{hexhash('rel2')}\n"),
        call(f"swh:1:rel:{hexhash('rel3')}\n"),
        call(f"swh:1:rel:{hexhash('rel4')}\n"),
    ]
    assert edge_writer.mock_calls == [
        call(f"swh:1:rel:{hexhash('rel1')} swh:1:rev:{hexhash('rev1')}\n"),
        call(f"swh:1:rel:{hexhash('rel2')} swh:1:rel:{hexhash('rel1')}\n"),
        call(f"swh:1:rel:{hexhash('rel3')} swh:1:dir:{hexhash('dir1')}\n"),
        call(f"swh:1:rel:{hexhash('rel4')} swh:1:cnt:{hexhash('cnt1')}\n"),
    ]


def test_export_revision(exporter):
    node_writer, edge_writer = exporter(
        {
            "revision": [
                {
                    **TEST_REVISION,
                    "id": binhash("rev1"),
                    "directory": binhash("dir1"),
                    "parents": [binhash("rev2"), binhash("rev3")],
                },
                {
                    **TEST_REVISION,
                    "id": binhash("rev2"),
                    "directory": binhash("dir2"),
                    "parents": [],
                },
            ]
        }
    )
    assert node_writer.mock_calls == [
        call(f"swh:1:rev:{hexhash('rev1')}\n"),
        call(f"swh:1:rev:{hexhash('rev2')}\n"),
    ]
    assert edge_writer.mock_calls == [
        call(f"swh:1:rev:{hexhash('rev1')} swh:1:dir:{hexhash('dir1')}\n"),
        call(f"swh:1:rev:{hexhash('rev1')} swh:1:rev:{hexhash('rev2')}\n"),
        call(f"swh:1:rev:{hexhash('rev1')} swh:1:rev:{hexhash('rev3')}\n"),
        call(f"swh:1:rev:{hexhash('rev2')} swh:1:dir:{hexhash('dir2')}\n"),
    ]


def test_export_directory(exporter):
    node_writer, edge_writer = exporter(
        {
            "directory": [
                {
                    "id": binhash("dir1"),
                    "entries": [
                        {
                            "type": "file",
                            "target": binhash("cnt1"),
                            "name": b"cnt1",
                            "perms": 0o644,
                        },
                        {
                            "type": "dir",
                            "target": binhash("dir2"),
                            "name": b"dir2",
                            "perms": 0o755,
                        },
                        {
                            "type": "rev",
                            "target": binhash("rev1"),
                            "name": b"rev1",
                            "perms": 0o160000,
                        },
                    ],
                },
                {"id": binhash("dir2"), "entries": []},
            ]
        }
    )
    assert node_writer.mock_calls == [
        call(f"swh:1:dir:{hexhash('dir1')}\n"),
        call(f"swh:1:dir:{hexhash('dir2')}\n"),
    ]
    assert edge_writer.mock_calls == [
        call(
            f"swh:1:dir:{hexhash('dir1')} swh:1:cnt:{hexhash('cnt1')}"
            f" {b64e('cnt1')} {0o644}\n"
        ),
        call(
            f"swh:1:dir:{hexhash('dir1')} swh:1:dir:{hexhash('dir2')}"
            f" {b64e('dir2')} {0o755}\n"
        ),
        call(
            f"swh:1:dir:{hexhash('dir1')} swh:1:rev:{hexhash('rev1')}"
            f" {b64e('rev1')} {0o160000}\n"
        ),
    ]


def test_export_content(exporter):
    node_writer, edge_writer = exporter(
        {
            "content": [
                {**TEST_CONTENT, "sha1_git": binhash("cnt1")},
                {**TEST_CONTENT, "sha1_git": binhash("cnt2")},
            ]
        }
    )
    assert node_writer.mock_calls == [
        call(f"swh:1:cnt:{hexhash('cnt1')}\n"),
        call(f"swh:1:cnt:{hexhash('cnt2')}\n"),
    ]
    assert edge_writer.mock_calls == []


def zstwrite(fp, lines):
    with ZSTFile(fp, "w") as writer:
        for line in lines:
            writer.write(line + "\n")


def zstread(fp):
    with ZSTFile(fp, "r") as reader:
        return reader.read()


def test_sort_pipeline(tmp_path):
    short_type_mapping = {
        "origin_visit_status": "ori",
        "snapshot": "snp",
        "release": "rel",
        "revision": "rev",
        "directory": "dir",
        "content": "cnt",
    }

    input_nodes = [
        f"swh:1:{short}:{hexhash(short + str(x))}"
        for short in short_type_mapping.values()
        for x in range(4)
    ]

    input_edges = [
        f"swh:1:ori:{hexhash('ori1')} swh:1:snp:{hexhash('snp1')}",
        f"swh:1:ori:{hexhash('ori2')} swh:1:snp:{hexhash('snp2')}",
        f"swh:1:ori:{hexhash('ori3')} swh:1:snp:{hexhash('snp3')}",
        f"swh:1:ori:{hexhash('ori4')} swh:1:snp:{hexhash('snpX')}",  # missing dest
        f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')} {b64e('dup1')}",
        f"swh:1:snp:{hexhash('snp1')} swh:1:rev:{hexhash('rev1')} {b64e('dup2')}",
        f"swh:1:snp:{hexhash('snp3')} swh:1:cnt:{hexhash('cnt1')} {b64e('c1')}",
        f"swh:1:snp:{hexhash('snp4')} swh:1:rel:{hexhash('rel1')} {b64e('r1')}",
        f"swh:1:rel:{hexhash('rel1')} swh:1:rel:{hexhash('rel2')}",
        f"swh:1:rel:{hexhash('rel2')} swh:1:rev:{hexhash('rev1')}",
        f"swh:1:rel:{hexhash('rel3')} swh:1:rev:{hexhash('rev2')}",
        f"swh:1:rel:{hexhash('rel4')} swh:1:dir:{hexhash('dir1')}",
        f"swh:1:rev:{hexhash('rev1')} swh:1:rev:{hexhash('rev1')}",  # dup
        f"swh:1:rev:{hexhash('rev1')} swh:1:rev:{hexhash('rev1')}",  # dup
        f"swh:1:rev:{hexhash('rev1')} swh:1:rev:{hexhash('rev2')}",
        f"swh:1:rev:{hexhash('rev2')} swh:1:rev:{hexhash('revX')}",  # missing dest
        f"swh:1:rev:{hexhash('rev3')} swh:1:rev:{hexhash('rev2')}",
        f"swh:1:rev:{hexhash('rev4')} swh:1:dir:{hexhash('dir1')}",
        f"swh:1:dir:{hexhash('dir1')} swh:1:cnt:{hexhash('cnt1')} {b64e('c1')} 42",
        f"swh:1:dir:{hexhash('dir1')} swh:1:dir:{hexhash('dir1')} {b64e('d1')} 1337",
        f"swh:1:dir:{hexhash('dir1')} swh:1:rev:{hexhash('rev1')} {b64e('r1')} 0",
    ]

    for obj_type, short_obj_type in short_type_mapping.items():
        p = tmp_path / obj_type
        p.mkdir()
        edges = [e for e in input_edges if e.startswith(f"swh:1:{short_obj_type}")]
        zstwrite(p / "00.edges.csv.zst", edges[0::2])
        zstwrite(p / "01.edges.csv.zst", edges[1::2])

        nodes = [n for n in input_nodes if n.startswith(f"swh:1:{short_obj_type}")]
        zstwrite(p / "00.nodes.csv.zst", nodes[0::2])
        zstwrite(p / "01.nodes.csv.zst", nodes[1::2])

    sort_graph_nodes(tmp_path, config={"sort_buffer_size": "1M"})

    output_nodes = zstread(tmp_path / "graph.nodes.csv.zst").split("\n")
    output_edges = zstread(tmp_path / "graph.edges.csv.zst").split("\n")
    output_labels = zstread(tmp_path / "graph.labels.csv.zst").split("\n")
    output_nodes = list(filter(bool, output_nodes))
    output_edges = list(filter(bool, output_edges))
    output_labels = list(filter(bool, output_labels))

    expected_nodes = set(input_nodes) | set(e.split()[1] for e in input_edges)
    assert output_nodes == sorted(expected_nodes)
    assert int((tmp_path / "graph.nodes.count.txt").read_text()) == len(expected_nodes)

    assert sorted(output_edges) == sorted(input_edges)
    assert int((tmp_path / "graph.edges.count.txt").read_text()) == len(input_edges)

    expected_labels = set(e[2] for e in [e.split() for e in input_edges] if len(e) > 2)
    assert output_labels == sorted(expected_labels)

    actual_node_stats = (tmp_path / "graph.nodes.stats.txt").read_text().strip()
    expected_node_stats = "\n".join(
        sorted(
            "{} {}".format(k, v)
            for k, v in collections.Counter(
                node.split(":")[2] for node in expected_nodes
            ).items()
        )
    )
    assert actual_node_stats == expected_node_stats

    actual_edge_stats = (tmp_path / "graph.edges.stats.txt").read_text().strip()
    expected_edge_stats = "\n".join(
        sorted(
            "{} {}".format(k, v)
            for k, v in collections.Counter(
                "{}:{}".format(edge.split(":")[2], edge.split(":")[5])
                for edge in input_edges
            ).items()
        )
    )
    assert actual_edge_stats == expected_edge_stats
