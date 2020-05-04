# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import os
import os.path
import pathlib
import shlex
import subprocess
import tempfile
import uuid

from swh.dataset.exporter import ParallelExporter
from swh.dataset.utils import ZSTFile
from swh.model.identifiers import origin_identifier, persistent_identifier
from swh.storage.fixer import fix_objects


def process_messages(messages, config, node_writer, edge_writer):
    """
    Args:
        messages: A sequence of messages to process
        config: The exporter configuration
        node_writer: A file-like object where to write nodes
        edge_writer: A file-like object where to write edges
    """

    def write_node(node):
        node_type, node_id = node
        if node_id is None:
            return
        node_pid = persistent_identifier(object_type=node_type, object_id=node_id)
        node_writer.write("{}\n".format(node_pid))

    def write_edge(src, dst):
        src_type, src_id = src
        dst_type, dst_id = dst
        if src_id is None or dst_id is None:
            return
        src_pid = persistent_identifier(object_type=src_type, object_id=src_id)
        dst_pid = persistent_identifier(object_type=dst_type, object_id=dst_id)
        edge_writer.write("{} {}\n".format(src_pid, dst_pid))

    messages = {k: fix_objects(k, v) for k, v in messages.items()}

    for visit in messages.get("origin_visit", []):
        origin_id = origin_identifier({"url": visit["origin"]})
        write_node(("origin", origin_id))
        write_edge(("origin", origin_id), ("snapshot", visit["snapshot"]))

    for snapshot in messages.get("snapshot", []):
        write_node(("snapshot", snapshot["id"]))
        for branch_name, branch in snapshot["branches"].items():
            while branch and branch.get("target_type") == "alias":
                branch_name = branch["target"]
                branch = snapshot["branches"][branch_name]
            if branch is None or not branch_name:
                continue
            if config.get("remove_pull_requests") and (
                branch_name.startswith(b"refs/pull")
                or branch_name.startswith(b"refs/merge-requests")
            ):
                continue
            write_edge(
                ("snapshot", snapshot["id"]), (branch["target_type"], branch["target"])
            )

    for release in messages.get("release", []):
        write_node(("release", release["id"]))
        write_edge(
            ("release", release["id"]), (release["target_type"], release["target"])
        )

    for revision in messages.get("revision", []):
        write_node(("revision", revision["id"]))
        write_edge(("revision", revision["id"]), ("directory", revision["directory"]))
        for parent in revision["parents"]:
            write_edge(("revision", revision["id"]), ("revision", parent))

    for directory in messages.get("directory", []):
        write_node(("directory", directory["id"]))
        for entry in directory["entries"]:
            entry_type_mapping = {
                "file": "content",
                "dir": "directory",
                "rev": "revision",
            }
            write_edge(
                ("directory", directory["id"]),
                (entry_type_mapping[entry["type"]], entry["target"]),
            )

    for content in messages.get("content", []):
        write_node(("content", content["sha1_git"]))


class GraphEdgeExporter(ParallelExporter):
    """
    Implementation of ParallelExporter which writes all the graph edges
    of a specific type in a Zstandard-compressed CSV file.

    Each row of the CSV is in the format: `<SRC PID> <DST PID>
    """

    def export_worker(self, export_path, **kwargs):
        dataset_path = pathlib.Path(export_path)
        dataset_path.mkdir(exist_ok=True, parents=True)
        nodes_file = dataset_path / ("graph-{}.nodes.csv.zst".format(str(uuid.uuid4())))
        edges_file = dataset_path / ("graph-{}.edges.csv.zst".format(str(uuid.uuid4())))

        with ZSTFile(nodes_file, "w") as nodes_writer, ZSTFile(
            edges_file, "w"
        ) as edges_writer:
            process_fn = functools.partial(
                process_messages,
                config=self.config,
                nodes_writer=nodes_writer,
                edges_writer=edges_writer,
            )
            self.process(process_fn, **kwargs)


def export_edges(config, export_path, export_id, processes):
    """Run the edge exporter for each edge type."""
    object_types = [
        "origin_visit",
        "snapshot",
        "release",
        "revision",
        "directory",
    ]
    for obj_type in object_types:
        print("{} edges:".format(obj_type))
        exporter = GraphEdgeExporter(config, export_id, obj_type, processes)
        exporter.run(os.path.join(export_path, obj_type))


def sort_graph_nodes(export_path, config):
    """
    Generate the node list from the edges files.

    We cannot solely rely on the object IDs that are read in the journal,
    as some nodes that are referred to as destinations in the edge file
    might not be present in the archive (e.g a rev_entry referring to a
    revision that we do not have crawled yet).

    The most efficient way of getting all the nodes that are mentioned in
    the edges file is therefore to use sort(1) on the gigantic edge files
    to get all the unique node IDs, while using the disk as a temporary
    buffer.

    This pipeline does, in order:

     - concatenate and write all the compressed edges files in
       graph.edges.csv.zst (using the fact that ZST compression is an additive
       function) ;
     - deflate the edges ;
     - count the number of edges and write it in graph.edges.count.txt ;
     - concatenate all the (deflated) nodes from the export with the
       destination edges, and sort the output to get the list of unique graph
       nodes ;
     - count the number of unique graph nodes and write it in
       graph.nodes.count.txt ;
     - compress and write the resulting nodes in graph.nodes.csv.zst.
    """
    # Use bytes for the sorting algorithm (faster than being locale-specific)
    env = {
        **os.environ.copy(),
        "LC_ALL": "C",
        "LC_COLLATE": "C",
        "LANG": "C",
    }
    sort_buffer_size = config.get("sort_buffer_size", "4G")
    disk_buffer_dir = config.get("disk_buffer_dir", export_path)
    with tempfile.TemporaryDirectory(
        prefix=".graph_node_sort_", dir=disk_buffer_dir
    ) as buffer_path:
        subprocess.run(
            [
                "bash",
                "-c",
                (
                    "pv {export_path}/*/*.edges.csv.zst | "
                    "tee {export_path}/graph.edges.csv.zst |"
                    "zstdcat |"
                    "tee >( wc -l > {export_path}/graph.edges.count.txt ) |"
                    "cut -d' ' -f2 | "
                    "cat - <( zstdcat {export_path}/*/*.nodes.csv.zst ) | "
                    "sort -u -S{sort_buffer_size} -T{buffer_path} | "
                    "tee >( wc -l > {export_path}/graph.nodes.count.txt ) |"
                    "zstdmt > {export_path}/graph.nodes.csv.zst"
                ).format(
                    export_path=shlex.quote(str(export_path)),
                    buffer_path=shlex.quote(str(buffer_path)),
                    sort_buffer_size=shlex.quote(sort_buffer_size),
                ),
            ],
            env=env,
        )
