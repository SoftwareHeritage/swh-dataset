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
from swh.dataset.relational import MAIN_TABLES


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
    """Dataset Tools.

    A set of tools to export datasets from the Software Heritage Archive in
    various formats.

    """
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
    "edges": "swh.dataset.exporters.edges:GraphEdgesExporter",
    "orc": "swh.dataset.exporters.orc:ORCExporter",
}


@graph.command("export")
@click.argument("export-path", type=click.Path())
@click.option(
    "--export-id",
    "-e",
    help=(
        "Unique ID of the export run. This is appended to the kafka "
        "group_id config file option. If group_id is not set in the "
        "'journal' section of the config file, defaults to 'swh-dataset-export-'."
    ),
)
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
@click.option(
    "--types",
    "object_types",
    type=click.STRING,
    help="Comma-separated list of objects types to export",
)
@click.option(
    "--margin",
    type=click.FloatRange(0, 1),
    help=(
        "Offset margin to start consuming from. E.g. is set to '0.95', "
        "consumers will start at 95% of the last committed offset; "
        "in other words, start earlier than last committed position."
    ),
)
@click.pass_context
def export_graph(
    ctx, export_path, export_id, formats, exclude, object_types, processes, margin
):
    """Export the Software Heritage graph as an edge dataset."""
    from importlib import import_module
    import uuid

    from swh.dataset.journalprocessor import ParallelJournalProcessor

    config = ctx.obj["config"]
    if not export_id:
        export_id = str(uuid.uuid4())

    if object_types:
        object_types = {o.strip() for o in object_types.split(",")}
        invalid_object_types = object_types - set(MAIN_TABLES.keys())
        if invalid_object_types:
            raise click.BadOptionUsage(
                option_name="types",
                message=f"Invalid object types: {', '.join(invalid_object_types)}.",
            )
    else:
        object_types = set(MAIN_TABLES.keys())
    exclude_obj_types = {o.strip() for o in (exclude.split(",") if exclude else [])}
    export_formats = [c.strip() for c in formats.split(",")]
    for f in export_formats:
        if f not in AVAILABLE_EXPORTERS:
            raise click.BadOptionUsage(
                option_name="formats", message=f"{f} is not an available format."
            )

    def importcls(clspath):
        mod, cls = clspath.split(":")
        m = import_module(mod)
        return getattr(m, cls)

    exporter_cls = dict(
        (fmt, importcls(clspath))
        for (fmt, clspath) in AVAILABLE_EXPORTERS.items()
        if fmt in export_formats
    )
    # Run the exporter for each edge type.
    for obj_type in object_types:
        if obj_type in exclude_obj_types:
            continue
        exporters = [
            (
                exporter_cls[f],
                {"export_path": os.path.join(export_path, f)},
            )
            for f in export_formats
        ]
        parallel_exporter = ParallelJournalProcessor(
            config,
            exporters,
            export_id,
            obj_type,
            node_sets_path=pathlib.Path(export_path) / ".node_sets" / obj_type,
            processes=processes,
            offset_margin=margin,
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
    database_name,
    query_file,
    output_location=None,
):
    """Query the AWS Athena database with a given command"""
    from swh.dataset.athena import run_query_get_results

    print(
        run_query_get_results(
            database_name,
            query_file.read(),
            output_location=output_location,
        ),
        end="",
    )  # CSV already ends with \n


@athena.command("gensubdataset")
@click.option("--database", "-d", default="swh", help="Name of the base database")
@click.option(
    "--subdataset-database",
    required=True,
    help="Name of the subdataset database to create",
)
@click.option(
    "--subdataset-location",
    required=True,
    help="S3 prefix where the subdataset should be stored",
)
@click.option(
    "--swhids",
    required=True,
    help="File containing the list of SWHIDs to include in the subdataset",
)
def athena_gensubdataset(database, subdataset_database, subdataset_location, swhids):
    """
    Generate a subdataset with Athena, from an existing database and a list
    of SWHIDs. Athena will generate a new dataset with the same tables as in
    the base dataset, but only containing the objects present in the SWHID
    list.
    """
    from swh.dataset.athena import generate_subdataset

    generate_subdataset(
        database,
        subdataset_database,
        subdataset_location,
        swhids,
        os.path.join(subdataset_location, "queries"),
    )
