# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import os
import os.path
import pathlib
import shlex
import subprocess
import tempfile
import uuid

from swh.dataset.exporter import ExporterDispatch
from swh.dataset.journalprocessor import ParallelJournalProcessor
from swh.dataset.utils import ZSTFile
from swh.model.identifiers import origin_identifier, swhid


class GraphEdgesExporter(ExporterDispatch):
    """
    Implementation of an exporter which writes all the graph edges
    of a specific type to a Zstandard-compressed CSV file.

    Each row of the CSV is in the format: `<SRC SWHID> <DST SWHID>
    """

    def __init__(self, config, export_path, **kwargs):
        super().__init__(config)
        self.export_path = export_path

    def __enter__(self):
        dataset_path = pathlib.Path(self.export_path)
        dataset_path.mkdir(exist_ok=True, parents=True)
        unique_id = str(uuid.uuid4())
        nodes_file = dataset_path / ("graph-{}.nodes.csv.zst".format(unique_id))
        edges_file = dataset_path / ("graph-{}.edges.csv.zst".format(unique_id))
        self.node_writer = ZSTFile(nodes_file, "w")
        self.edge_writer = ZSTFile(edges_file, "w")
        self.node_writer.__enter__()
        self.edge_writer.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.node_writer.__exit__(exc_type, exc_value, traceback)
        self.edge_writer.__exit__(exc_type, exc_value, traceback)

    def write_node(self, node):
        node_type, node_id = node
        if node_id is None:
            return
        node_swhid = swhid(object_type=node_type, object_id=node_id)
        self.node_writer.write("{}\n".format(node_swhid))

    def write_edge(self, src, dst, *, labels=None):
        src_type, src_id = src
        dst_type, dst_id = dst
        if src_id is None or dst_id is None:
            return
        src_swhid = swhid(object_type=src_type, object_id=src_id)
        dst_swhid = swhid(object_type=dst_type, object_id=dst_id)
        edge_line = " ".join([src_swhid, dst_swhid] + (labels if labels else []))
        self.edge_writer.write("{}\n".format(edge_line))

    def process_origin(self, origin):
        origin_id = origin_identifier({"url": origin["url"]})
        self.write_node(("origin", origin_id))

    def process_origin_visit_status(self, visit_status):
        origin_id = origin_identifier({"url": visit_status["origin"]})
        self.write_edge(("origin", origin_id), ("snapshot", visit_status["snapshot"]))

    def process_snapshot(self, snapshot):
        if self.config.get("remove_pull_requests"):
            self.remove_pull_requests(snapshot)

        self.write_node(("snapshot", snapshot["id"]))
        for branch_name, branch in snapshot["branches"].items():
            original_branch_name = branch_name
            while branch and branch.get("target_type") == "alias":
                branch_name = branch["target"]
                branch = snapshot["branches"][branch_name]
            if branch is None or not branch_name:
                continue
            self.write_edge(
                ("snapshot", snapshot["id"]),
                (branch["target_type"], branch["target"]),
                labels=[base64.b64encode(original_branch_name).decode(),],
            )

    def process_release(self, release):
        self.write_node(("release", release["id"]))
        self.write_edge(
            ("release", release["id"]), (release["target_type"], release["target"])
        )

    def process_revision(self, revision):
        self.write_node(("revision", revision["id"]))
        self.write_edge(
            ("revision", revision["id"]), ("directory", revision["directory"])
        )
        for parent in revision["parents"]:
            self.write_edge(("revision", revision["id"]), ("revision", parent))

    def process_directory(self, directory):
        self.write_node(("directory", directory["id"]))
        for entry in directory["entries"]:
            entry_type_mapping = {
                "file": "content",
                "dir": "directory",
                "rev": "revision",
            }
            self.write_edge(
                ("directory", directory["id"]),
                (entry_type_mapping[entry["type"]], entry["target"]),
                labels=[base64.b64encode(entry["name"]).decode(), str(entry["perms"]),],
            )

    def process_content(self, content):
        self.write_node(("content", content["sha1_git"]))

    def remove_pull_requests(self, snapshot):
        """
        Heuristic to filter out pull requests in snapshots: remove all branches
        that start with refs/ but do not start with refs/heads or refs/tags.
        """
        # Copy the items with list() to remove items during iteration
        for branch_name, branch in list(snapshot["branches"].items()):
            original_branch_name = branch_name
            while branch and branch.get("target_type") == "alias":
                branch_name = branch["target"]
                branch = snapshot["branches"][branch_name]
            if branch is None or not branch_name:
                continue
            if branch_name.startswith(b"refs/") and not (
                branch_name.startswith(b"refs/heads")
                or branch_name.startswith(b"refs/tags")
            ):
                snapshot["branches"].pop(original_branch_name)


def export_edges(config, export_path, export_id, processes):
    """Run the edge exporter for each edge type."""
    object_types = [
        "origin",
        "origin_visit_status",
        "snapshot",
        "release",
        "revision",
        "directory",
        "content",
    ]
    for obj_type in object_types:
        print("{} edges:".format(obj_type))
        exporters = [
            (GraphEdgesExporter, {"export_path": os.path.join(export_path, obj_type)}),
        ]
        parallel_exporter = ParallelJournalProcessor(
            config,
            exporters,
            export_id,
            obj_type,
            node_sets_path=pathlib.Path(export_path) / ".node_sets",
            processes=processes,
        )
        parallel_exporter.run()


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
     - count the number of occurrences of each edge type and write them
       in graph.edges.stats.txt ;
     - concatenate all the (deflated) nodes from the export with the
       destination edges, and sort the output to get the list of unique graph
       nodes ;
     - count the number of unique graph nodes and write it in
       graph.nodes.count.txt ;
     - count the number of occurrences of each node type and write them
       in graph.nodes.stats.txt ;
     - compress and write the resulting nodes in graph.nodes.csv.zst.
    """

    # Use awk as a replacement of `sort | uniq -c` to avoid buffering everything
    # in memory
    counter_command = "awk '{ t[$0]++ } END { for (i in t) print i,t[i] }'"

    sort_script = """
    pv {export_path}/*/*.edges.csv.zst |
        tee {export_path}/graph.edges.csv.zst |
        zstdcat |
        tee >( wc -l > {export_path}/graph.edges.count.txt ) |
        tee >( cut -d: -f3,6 | {counter_command} | sort \
                   > {export_path}/graph.edges.stats.txt ) |
        tee >( cut -d' ' -f3 | grep . | \
                   sort -u -S{sort_buffer_size} -T{buffer_path} | \
                   zstdmt > {export_path}/graph.labels.csv.zst ) |
        cut -d' ' -f2 |
        cat - <( zstdcat {export_path}/*/*.nodes.csv.zst ) |
        sort -u -S{sort_buffer_size} -T{buffer_path} |
        tee >( wc -l > {export_path}/graph.nodes.count.txt ) |
        tee >( cut -d: -f3 | {counter_command} | sort \
                   > {export_path}/graph.nodes.stats.txt ) |
        zstdmt > {export_path}/graph.nodes.csv.zst
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
                sort_script.format(
                    export_path=shlex.quote(str(export_path)),
                    buffer_path=shlex.quote(str(buffer_path)),
                    sort_buffer_size=shlex.quote(sort_buffer_size),
                    counter_command=counter_command,
                ),
            ],
            env=env,
        )
