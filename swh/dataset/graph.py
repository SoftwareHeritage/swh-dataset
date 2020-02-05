# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import os
import pathlib
import shlex
import subprocess
import tempfile
import uuid

from swh.dataset.exporter import ParallelExporter
from swh.dataset.utils import ZSTWriter
from swh.model.identifiers import origin_identifier, persistent_identifier


def process_messages(messages, writer, config):
    def write(src, dst):
        src_type, src_id = src
        dst_type, dst_id = dst
        if src_id is None or dst_id is None:
            return
        src_pid = persistent_identifier(object_type=src_type, object_id=src_id)
        dst_pid = persistent_identifier(object_type=dst_type, object_id=dst_id)
        writer.write('{} {}\n'.format(src_pid, dst_pid))

    for visit in messages.get('origin_visit', []):
        write(('origin', origin_identifier({'url': visit['origin']['url']})),
              ('snapshot', visit['snapshot']))

    for snapshot in messages.get('snapshot', []):
        for branch_name, branch in snapshot['branches'].items():
            while branch and branch.get('target_type') == 'alias':
                branch_name = branch['target']
                branch = snapshot['branches'][branch_name]
            if branch is None or not branch_name:
                continue
            if (config.get('remove_pull_requests')
                    and branch_name.startswith(b'refs/pull')
                    or branch_name.startswith(b'refs/merge-requests')):
                continue
            write(('snapshot', snapshot['id']),
                  (branch['target_type'], branch['target']))

    for release in messages.get('release', []):
        write(('release', release['id']),
              (release['target_type'], release['target']))

    for revision in messages.get('revision', []):
        write(('revision', revision['id']),
              ('directory', revision['directory']))
        for parent in revision['parents']:
            write(('revision', revision['id']),
                  ('revision', parent))

    for directory in messages.get('directory', []):
        for entry in directory['entries']:
            entry_type_mapping = {
                'file': 'content',
                'dir': 'directory',
                'rev': 'revision'
            }
            write(('directory', directory['id']),
                  (entry_type_mapping[entry['type']], entry['target']))


class GraphEdgeExporter(ParallelExporter):
    def export_worker(self, export_path, **kwargs):
        dataset_path = pathlib.Path(export_path)
        dataset_path.mkdir(exist_ok=True, parents=True)
        dataset_file = dataset_path / ('graph-{}.edges.csv.zst'
                                       .format(str(uuid.uuid4())))

        with ZSTWriter(dataset_file) as writer:
            process_fn = functools.partial(
                process_messages, writer=writer, config=self.config,
            )
            self.process(process_fn, **kwargs)


def export_edges(config, export_path, export_id, processes):
    object_types = [
        'origin_visit',
        'snapshot',
        'release',
        'revision',
        'directory',
    ]
    for obj_type in object_types:
        print('{} edges:'.format(obj_type))
        exporter = GraphEdgeExporter(config, export_id, obj_type, processes)
        exporter.run(export_path)


def sort_graph_nodes(export_path, config):
    # Use bytes for the sorting algorithm (faster than being locale-specific)
    env = {
        **os.environ.copy(),
        'LC_ALL': 'C',
        'LC_COLLATE': 'C',
        'LANG': 'C',
    }
    sort_buffer_size = config.get('sort_buffer_size', '4G')
    disk_buffer_dir = config.get('disk_buffer_dir', export_path)
    with tempfile.TemporaryDirectory(prefix='.graph_node_sort_',
                                     dir=disk_buffer_dir) as buffer_path:
        subprocess.run(
            ("zstdcat {export_path}/*.edges.csv.zst | "
             "tr ' ' '\\n' | "
             "sort -u -S{sort_buffer_size} -T{buffer_path} | "
             "zstdmt > {export_path}/graph.nodes.csv.zst")
            .format(
                export_path=shlex.quote(export_path),
                buffer_path=shlex.quote(buffer_path),
                sort_buffer_size=shlex.quote(sort_buffer_size),
            ),
            shell=True,
            env=env,
        )
