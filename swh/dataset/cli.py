# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import click

from swh.core.cli import CONTEXT_SETTINGS, swh as swh_cli_group


@swh_cli_group.group(name="dataset", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Configuration file.",
)
@click.pass_context
def dataset_cli_group(ctx, config_file):
    """Software Heritage Dataset Tools"""
    from swh.core import config

    ctx.ensure_object(dict)
    conf = config.read(config_file)
    ctx.obj["config"] = conf


@dataset_cli_group.group("graph")
@click.pass_context
def graph(ctx):
    """Manage graph edges export"""
    pass


@graph.command("export")
@click.argument("export-path", type=click.Path())
@click.option("--export-id", "-e", help="Unique ID of the export run.")
@click.option("--processes", "-p", default=1, help="Number of parallel processes")
@click.pass_context
def export_graph(ctx, export_path, export_id, processes):
    """Export the Software Heritage graph as an edge dataset."""
    import uuid

    config = ctx.obj["config"]
    if not export_id:
        export_id = str(uuid.uuid4())

    from swh.dataset.graph import export_edges

    export_edges(config, export_path, export_id, processes)


@graph.command("sort")
@click.argument("export-path", type=click.Path())
@click.pass_context
def sort_graph(ctx, export_path):
    config = ctx.obj["config"]
    from swh.dataset.graph import sort_graph_nodes

    sort_graph_nodes(export_path, config)
