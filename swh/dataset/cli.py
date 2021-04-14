# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import os
import pathlib
import sys

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group
from swh.dataset.exporters.edges import GraphEdgesExporter
from swh.dataset.exporters.orc import ORCExporter
from swh.dataset.journalprocessor import ParallelJournalProcessor


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
    """Manage graph export"""
    pass


AVAILABLE_EXPORTERS = {
    "edges": GraphEdgesExporter,
    "orc": ORCExporter,
}


@graph.command("export")
@click.argument("export-path", type=click.Path())
@click.option("--export-id", "-e", help="Unique ID of the export run.")
@click.option(
    "--formats",
    "-f",
    type=click.STRING,
    default=",".join(AVAILABLE_EXPORTERS.keys()),
    show_default=True,
    help="Formats to export.",
)
@click.option("--processes", "-p", default=1, help="Number of parallel processes")
@click.option(
    "--exclude",
    type=click.STRING,
    help="Comma-separated list of object types to exclude",
)
@click.pass_context
def export_graph(ctx, export_path, export_id, formats, exclude, processes):
    """Export the Software Heritage graph as an edge dataset."""
    import uuid

    config = ctx.obj["config"]
    if not export_id:
        export_id = str(uuid.uuid4())

    exclude_obj_types = {o.strip() for o in (exclude.split(",") if exclude else [])}
    export_formats = [c.strip() for c in formats.split(",")]
    for f in export_formats:
        if f not in AVAILABLE_EXPORTERS:
            raise click.BadOptionUsage(
                option_name="formats", message=f"{f} is not an available format."
            )

    # Run the exporter for each edge type.
    object_types = [
        "origin",
        "origin_visit",
        "origin_visit_status",
        "snapshot",
        "release",
        "revision",
        "directory",
        "content",
        "skipped_content",
    ]
    for obj_type in object_types:
        if obj_type in exclude_obj_types:
            continue
        exporters = [
            (AVAILABLE_EXPORTERS[f], {"export_path": os.path.join(export_path, f)},)
            for f in export_formats
        ]
        parallel_exporter = ParallelJournalProcessor(
            config,
            exporters,
            export_id,
            obj_type,
            node_sets_path=pathlib.Path(export_path) / ".node_sets" / obj_type,
            processes=processes,
        )
        print("Exporting {}:".format(obj_type))
        parallel_exporter.run()


@graph.command("sort")
@click.argument("export-path", type=click.Path())
@click.pass_context
def sort_graph(ctx, export_path):
    config = ctx.obj["config"]
    from swh.dataset.exporters.edges import sort_graph_nodes

    sort_graph_nodes(export_path, config)


@dataset_cli_group.group("athena")
@click.pass_context
def athena(ctx):
    """Manage and query a remote AWS Athena database"""
    pass


@athena.command("create")
@click.option(
    "--database-name", "-d", default="swh", help="Name of the database to create"
)
@click.option(
    "--location-prefix",
    "-l",
    required=True,
    help="S3 prefix where the dataset can be found",
)
@click.option(
    "-o", "--output-location", help="S3 prefix where results should be stored"
)
@click.option(
    "-r", "--replace-tables", is_flag=True, help="Replace the tables that already exist"
)
def athena_create(
    database_name, location_prefix, output_location=None, replace_tables=False
):
    """Create tables on AWS Athena pointing to a given graph dataset on S3."""
    from swh.dataset.athena import create_tables

    create_tables(
        database_name,
        location_prefix,
        output_location=output_location,
        replace=replace_tables,
    )


@athena.command("query")
@click.option(
    "--database-name", "-d", default="swh", help="Name of the database to query"
)
@click.option(
    "-o", "--output-location", help="S3 prefix where results should be stored"
)
@click.argument("query_file", type=click.File("r"), default=sys.stdin)
def athena_query(
    database_name, query_file, output_location=None,
):
    """Query the AWS Athena database with a given command"""
    from swh.dataset.athena import run_query_get_results

    print(
        run_query_get_results(
            database_name, query_file.read(), output_location=output_location,
        ),
        end="",
    )  # CSV already ends with \n
