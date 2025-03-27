# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import os
import pathlib
import sys
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group

from .relational import MAIN_TABLES

if TYPE_CHECKING:
    from swh.model.swhids import ExtendedSWHID


@swh_cli_group.group(name="export", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Configuration file.",
)
@click.pass_context
def export_cli_group(ctx, config_file):
    """Dataset Export Tools.

    A set of tools to export datasets from the Software Heritage Archive in
    various formats.

    """
    from swh.core import config

    ctx.ensure_object(dict)
    conf = config.read(config_file)
    ctx.obj["config"] = conf


@export_cli_group.group("graph")
@click.pass_context
def graph(ctx):
    """Manage graph export"""
    pass


AVAILABLE_EXPORTERS = {
    "edges": "swh.export.exporters.edges:GraphEdgesExporter",
    "orc": "swh.export.exporters.orc:ORCExporter",
}


@graph.command("export")
@click.argument("export-path", type=click.Path())
@click.option(
    "--export-id",
    "-e",
    help=(
        "Unique ID of the export run. This is appended to the kafka "
        "group_id config file option. If group_id is not set in the "
        "'journal' section of the config file, defaults to 'swh-export-export-'."
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
@click.option(
    "--sensitive-export-path",
    type=click.STRING,
    help="Path where sensitive data (eg. fullnames) will be exported to.",
)
@click.pass_context
def export_graph(
    ctx, export_path, sensitive_export_path, formats, exclude, object_types, **kwargs
):
    """Export the Software Heritage graph as an edge dataset."""

    config = ctx.obj["config"]
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
    export_path = pathlib.Path(export_path)
    if sensitive_export_path is not None:
        sensitive_export_path = pathlib.Path(sensitive_export_path)
    else:
        sensitive_export_path = export_path.parent / f"{export_path.name}-sensitive"

    run_export_graph(
        config,
        pathlib.Path(export_path),
        pathlib.Path(sensitive_export_path),
        export_formats,
        list(object_types),
        exclude_obj_types=exclude_obj_types,
        **kwargs,
    )


def get_masked_swhids(logger, config: Dict[str, Any]) -> Set["ExtendedSWHID"]:
    """Fetches the masking database and returns the list of all non-visible SWHIDs"""
    import tqdm

    from swh.storage.proxies.masking.db import MaskingQuery

    if config["masking_db"] is None:
        logger.warning("Exporting dataset without masking.")
        return set()
    masking_query = MaskingQuery.connect(config["masking_db"])
    return {
        swhid
        for (swhid, statuses) in tqdm.tqdm(
            masking_query.iter_masked_swhids(),
            desc="Listing masked SWHIDs",
            unit_scale=True,
        )
    }


def run_export_graph(
    config: Dict[str, Any],
    export_path: pathlib.Path,
    sensitive_export_path: pathlib.Path,
    export_formats: List[str],
    object_types: List[str],
    exclude_obj_types: Set[str],
    export_id: Optional[str],
    processes: int,
    margin: Optional[float],
):
    import functools
    from importlib import import_module
    import logging
    import resource
    import tempfile
    import uuid

    from .fullnames import process_fullnames
    from .journalprocessor import ParallelJournalProcessor

    logger = logging.getLogger(__name__)

    masked_swhids = get_masked_swhids(logger, config)

    if not export_id:
        export_id = str(uuid.uuid4())

    # Enforce order (from origin to contents) to reduce number of holes in the graph.
    object_types = [
        obj_type for obj_type in MAIN_TABLES.keys() if obj_type in object_types
    ]

    # ParallelJournalProcessor opens 256 LevelDBs in total. Depending on the number of
    # processes, this can exceed the maximum number of file descriptors (soft limit
    # defaults to 1024 on Debian), so let's increase it.
    (soft, hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
    nb_shards = 256  # TODO: make this configurable or detect nb of kafka partitions
    open_fds_per_shard = 61  # estimated with plyvel==1.3.0 and libleveldb1d==1.22-3
    spare = 1024  # for everything other than LevelDB
    want_fd = nb_shards * open_fds_per_shard + spare
    if hard < want_fd:
        logger.warning(
            "Hard limit of open file descriptors (%d) is lower than ideal (%d)",
            hard,
            want_fd,
        )
    if soft < want_fd:
        want_fd = min(want_fd, hard)
        logger.info(
            "Soft limit of open file descriptors (%d) is too low, increasing to %d",
            soft,
            want_fd,
        )
        resource.setrlimit(resource.RLIMIT_NOFILE, (want_fd, hard))

    def importcls(clspath):
        mod, cls = clspath.split(":")
        m = import_module(mod)
        return getattr(m, cls)

    exporter_cls = dict(
        (fmt, importcls(clspath))
        for (fmt, clspath) in AVAILABLE_EXPORTERS.items()
        if fmt in export_formats
    )

    with tempfile.TemporaryDirectory() as csv_dir:
        dup_dir = pathlib.Path(csv_dir) / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        dedup_dir = pathlib.Path(csv_dir) / "deduplicated"
        dedup_dir.mkdir(parents=True, exist_ok=True)

        # Run the exporter for each edge type.
        parallel_exporters = {}
        for obj_type in object_types:
            if obj_type in exclude_obj_types:
                continue
            exporters = [
                functools.partial(
                    exporter_cls[f],
                    config=config,
                    export_path=export_path / f,
                    sensitive_export_path=sensitive_export_path / f,
                )
                for f in export_formats
            ]
            parallel_exporters[obj_type] = ParallelJournalProcessor(
                config,
                masked_swhids,
                exporters,
                export_id,
                obj_type,
                node_sets_path=export_path / ".node_sets",
                dup_dir=dup_dir,
                dedup_dir=dedup_dir,
                processes=processes,
                offset_margin=margin,
            )
            # Fetch all offsets before we start exporting to minimize the time interval
            # between the offsets of each topic
            parallel_exporters[obj_type].get_offsets()

        for obj_type, parallel_exporter in parallel_exporters.items():
            parallel_exporter.run()

        if config["journal"].get("privileged"):
            fullnames_export_path = sensitive_export_path / "orc" / "person"
            fullnames_export_path.mkdir(parents=True, exist_ok=True)
            fullnames_orc = fullnames_export_path / f"{uuid.uuid4()}.orc"
            process_fullnames(fullnames_orc, dedup_dir)


@graph.command("sort")
@click.argument("export-path", type=click.Path())
@click.pass_context
def sort_graph(ctx, export_path):
    config = ctx.obj["config"]
    from .exporters.edges import sort_graph_nodes

    sort_graph_nodes(export_path, config)


@export_cli_group.group("athena")
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
    from .athena import create_tables

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
    from .athena import run_query_get_results

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
    from .athena import generate_subdataset

    generate_subdataset(
        database,
        subdataset_database,
        subdataset_location,
        swhids,
        os.path.join(subdataset_location, "queries"),
    )
