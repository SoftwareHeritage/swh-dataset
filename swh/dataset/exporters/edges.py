# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import os
import os.path
import shlex
import subprocess
import tempfile
from typing import Tuple

from swh.dataset.exporter import ExporterDispatch
from swh.dataset.utils import ZSTFile, remove_pull_requests
from swh.model.hashutil import hash_to_hex
from swh.model.model import Origin
from swh.model.swhids import ExtendedObjectType


def swhid(object_type, object_id):
    # We use string interpolation here instead of using ExtendedSWHID to format,
    # as building temporary ExtendedSWHID objects has a non-negligeable impact
    # on performance.
    return f"swh:1:{object_type.value}:{hash_to_hex(object_id)}"


class GraphEdgesExporter(ExporterDispatch):
    """
    Implementation of an exporter which writes all the graph edges
    of a specific type to a Zstandard-compressed CSV file.

    Each row of the CSV is in the format: ``<SRC SWHID> <DST SWHID>``.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writers = {}

    def get_writers_for(self, obj_type: ExtendedObjectType):
        if obj_type not in self.writers:
            dataset_path = self.export_path / obj_type.name.lower()
            dataset_path.mkdir(exist_ok=True)
            unique_id = self.get_unique_file_id()
            nodes_file = dataset_path / ("graph-{}.nodes.csv.zst".format(unique_id))
            edges_file = dataset_path / ("graph-{}.edges.csv.zst".format(unique_id))
            node_writer = self.exit_stack.enter_context(ZSTFile(str(nodes_file), "w"))
            edge_writer = self.exit_stack.enter_context(ZSTFile(str(edges_file), "w"))
            self.writers[obj_type] = (node_writer, edge_writer)
        return self.writers[obj_type]

    def get_node_writer_for(self, obj_type: ExtendedObjectType):
        return self.get_writers_for(obj_type)[0]

    def get_edge_writer_for(self, obj_type: ExtendedObjectType):
        return self.get_writers_for(obj_type)[1]

    def write_node(self, node: Tuple[ExtendedObjectType, bytes]):
        node_type, node_id = node
        if node_id is None:
            return
        node_swhid = swhid(object_type=node_type, object_id=node_id)
        node_writer = self.get_node_writer_for(node_type)
        node_writer.write("{}\n".format(node_swhid))

    def write_edge(
        self,
        src: Tuple[ExtendedObjectType, bytes],
        dst: Tuple[ExtendedObjectType, bytes],
        *,
        labels=None,
    ):
        src_type, src_id = src
        dst_type, dst_id = dst
        if src_id is None or dst_id is None:
            return
        src_swhid = swhid(object_type=src_type, object_id=src_id)
        dst_swhid = swhid(object_type=dst_type, object_id=dst_id)
        edge_line = " ".join([src_swhid, dst_swhid] + (labels if labels else []))
        edge_writer = self.get_edge_writer_for(src_type)
        edge_writer.write("{}\n".format(edge_line))

    def process_origin(self, origin):
        origin_id = Origin(url=origin["url"]).id
        self.write_node((ExtendedObjectType.ORIGIN, origin_id))

    def process_origin_visit_status(self, visit_status):
        origin_id = Origin(url=visit_status["origin"]).id
        self.write_edge(
            (ExtendedObjectType.ORIGIN, origin_id),
            (ExtendedObjectType.SNAPSHOT, visit_status["snapshot"]),
        )

    def process_snapshot(self, snapshot):
        if self.config.get("remove_pull_requests"):
            remove_pull_requests(snapshot)

        self.write_node((ExtendedObjectType.SNAPSHOT, snapshot["id"]))
        for branch_name, branch in snapshot["branches"].items():
            original_branch_name = branch_name
            while branch and branch.get("target_type") == "alias":
                branch_name = branch["target"]
                branch = snapshot["branches"].get(branch_name)
            if branch is None or not branch_name:
                continue
            self.write_edge(
                (ExtendedObjectType.SNAPSHOT, snapshot["id"]),
                (ExtendedObjectType[branch["target_type"].upper()], branch["target"]),
                labels=[
                    base64.b64encode(original_branch_name).decode(),
                ],
            )

    def process_release(self, release):
        self.write_node((ExtendedObjectType.RELEASE, release["id"]))
        self.write_edge(
            (ExtendedObjectType.RELEASE, release["id"]),
            (ExtendedObjectType[release["target_type"].upper()], release["target"]),
        )

    def process_revision(self, revision):
        self.write_node((ExtendedObjectType.REVISION, revision["id"]))
        self.write_edge(
            (ExtendedObjectType.REVISION, revision["id"]),
            (ExtendedObjectType.DIRECTORY, revision["directory"]),
        )
        for parent in revision["parents"]:
            self.write_edge(
                (ExtendedObjectType.REVISION, revision["id"]),
                (ExtendedObjectType.REVISION, parent),
            )

    def process_directory(self, directory):
        self.write_node((ExtendedObjectType.DIRECTORY, directory["id"]))
        for entry in directory["entries"]:
            entry_type_mapping = {
                "file": ExtendedObjectType.CONTENT,
                "dir": ExtendedObjectType.DIRECTORY,
                "rev": ExtendedObjectType.REVISION,
            }
            self.write_edge(
                (ExtendedObjectType.DIRECTORY, directory["id"]),
                (entry_type_mapping[entry["type"]], entry["target"]),
                labels=[base64.b64encode(entry["name"]).decode(), str(entry["perms"])],
            )

    def process_content(self, content):
        self.write_node((ExtendedObjectType.CONTENT, content["sha1_git"]))


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
