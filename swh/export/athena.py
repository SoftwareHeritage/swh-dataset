# Copyright (C) 2021-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
This module implements the "athena" subcommands for the CLI. It can install and
query a remote AWS Athena database.
"""

import datetime
import logging
import os
import sys
import textwrap
import time
from pathlib import Path

import boto3
import botocore.exceptions

from .relational import TABLES

# Join field for each table when filtering by SWHID hash.
# Tables where join field is just a column name (same in all SQL dialects)
_TABLES_JOIN_FIELD_SIMPLE = [
    ("snapshot", "id"),
    ("snapshot_branch", "snapshot_id"),
    ("release", "id"),
    ("revision", "id"),
    ("revision_history", "id"),
    ("revision_extra_headers", "id"),
    ("directory", "id"),
    ("directory_entry", "directory_id"),
    ("content", "sha1_git"),
    ("skipped_content", "sha1_git"),
]

# Origin tables: Athena computes hash from URL, DataFusion uses joins
_ORIGIN_TABLES_JOIN_FIELD = {
    "athena": [
        ("origin", "lower(to_hex(sha1(to_utf8(url))))"),
        ("origin_visit", "lower(to_hex(sha1(to_utf8(origin))))"),
        ("origin_visit_status", "lower(to_hex(sha1(to_utf8(origin))))"),
    ],
    "datafusion": [
        # origin table has id = sha1(url), use it directly
        ("origin", "id"),
        # origin_visit/status join with filtered origin table instead of computing hash
        ("origin_visit", "origin"),
        ("origin_visit_status", "origin"),
    ],
}


def get_tables_join_field(dialect: str):
    """Return (table, join_field) pairs for the given SQL dialect."""
    return _ORIGIN_TABLES_JOIN_FIELD[dialect] + _TABLES_JOIN_FIELD_SIMPLE


def create_database(database_name):
    return "CREATE DATABASE IF NOT EXISTS {};".format(database_name)


def drop_table(database_name, table):
    return "DROP TABLE IF EXISTS {}.{};".format(database_name, table)


def create_table(database_name, table, location_prefix):
    req = textwrap.dedent(
        """\
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} (
        {fields}
        )
        STORED AS ORC
        LOCATION '{location}/'
        TBLPROPERTIES ("orc.compress"="ZSTD");
        """
    ).format(
        db=database_name,
        table=table,
        fields=",\n".join(
            [
                "    `{}` {}".format(col_name, col_type)
                for col_name, col_type in TABLES[table]
            ]
        ),
        location=os.path.join(location_prefix, "orc", table),
    )
    return req


def repair_table(database_name, table):
    return "MSCK REPAIR TABLE {}.{};".format(database_name, table)


def query(client, query_string, *, desc="Querying", delay_secs=0.5, silent=False):
    def log(*args, **kwargs):
        if not silent:
            print(*args, **kwargs, flush=True, file=sys.stderr)

    log(desc, end="...")
    query_options = {
        "QueryString": query_string,
        "ResultConfiguration": {},
        "QueryExecutionContext": {},
    }
    if client.output_location:
        query_options["ResultConfiguration"]["OutputLocation"] = client.output_location
    if client.database_name:
        query_options["QueryExecutionContext"]["Database"] = client.database_name
    try:
        res = client.start_query_execution(**query_options)
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(
            str(e) + "\n\nQuery:\n" + textwrap.indent(query_string, " " * 2)
        )
    qid = res["QueryExecutionId"]
    while True:
        time.sleep(delay_secs)
        log(".", end="")
        execution = client.get_query_execution(QueryExecutionId=qid)
        status = execution["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
    log(" {}.".format(status["State"]))
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(
            status["StateChangeReason"]
            + "\n\nQuery:\n"
            + textwrap.indent(query_string, " " * 2)
        )

    return execution["QueryExecution"]


def create_tables(database_name, dataset_location, output_location=None, replace=False):
    """
    Create the Software Heritage Dataset tables on AWS Athena.

    Athena works on external columnar data stored in S3, but requires a schema
    for each table to run queries. This creates all the necessary tables
    remotely by using the relational schemas in swh.export.relational.
    """
    client = boto3.client("athena")
    client.output_location = output_location

    client.database_name = "default"  # we have to pick some existing database
    query(
        client,
        create_database(database_name),
        desc="Creating {} database".format(database_name),
    )
    client.database_name = database_name

    if replace:
        for table in TABLES:
            query(
                client,
                drop_table(database_name, table),
                desc="Dropping table {}".format(table),
            )

    for table in TABLES:
        query(
            client,
            create_table(database_name, table, dataset_location),
            desc="Creating table {}".format(table),
        )

    for table in TABLES:
        query(
            client,
            repair_table(database_name, table),
            desc="Refreshing table metadata for {}".format(table),
        )


def human_size(n, units=["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]):
    """Returns a human readable string representation of bytes"""
    return f"{n} " + units[0] if n < 1024 else human_size(n >> 10, units[1:])


def _s3_url_to_bucket_path(s3_url):
    loc = s3_url.removeprefix("s3://")
    bucket, path = loc.split("/", 1)
    return bucket, path


def run_query_get_results(
    database_name,
    query_string,
    output_location=None,
):
    """
    Run a query on AWS Athena and return the resulting data in CSV format.
    """
    athena = boto3.client("athena")
    athena.output_location = output_location
    athena.database_name = database_name

    s3 = boto3.client("s3")

    result = query(athena, query_string, silent=True)
    logging.info(
        "Scanned %s in %s",
        human_size(result["Statistics"]["DataScannedInBytes"]),
        datetime.timedelta(
            milliseconds=result["Statistics"]["TotalExecutionTimeInMillis"]
        ),
    )

    bucket, path = _s3_url_to_bucket_path(
        result["ResultConfiguration"]["OutputLocation"]
    )
    return s3.get_object(Bucket=bucket, Key=path)["Body"].read().decode()


def generate_subdataset(
    dataset_db,
    subdataset_db,
    subdataset_s3_path,
    swhids_file,
    output_location=None,
):
    # Upload list of all the swhids included in the dataset
    subdataset_bucket, subdataset_path = _s3_url_to_bucket_path(subdataset_s3_path)
    s3_client = boto3.client("s3")
    print(f"Uploading {swhids_file} to S3...")
    s3_client.upload_file(
        swhids_file,
        subdataset_bucket,
        os.path.join(subdataset_path, "swhids", "swhids.csv"),
    )

    athena_client = boto3.client("athena")
    athena_client.output_location = output_location
    athena_client.database_name = subdataset_db

    # Create subdataset database
    query(
        athena_client,
        create_database(subdataset_db),
        desc="Creating {} database".format(subdataset_db),
    )

    # Create SWHID temporary table
    create_swhid_table_query = textwrap.dedent(
        """\
        CREATE EXTERNAL TABLE IF NOT EXISTS {newdb}.swhids (
            swhprefix string,
            version int,
            type string,
            hash string
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ':'
        STORED AS TEXTFILE
        LOCATION '{location}/swhids/'
        """
    ).format(newdb=subdataset_db, location=subdataset_s3_path)
    query(
        athena_client,
        create_swhid_table_query,
        desc="Creating SWHIDs table of subdataset",
    )
    query(
        athena_client,
        repair_table(subdataset_db, "swhids"),
        desc="Refreshing table metadata for swhids table",
    )

    # Create join tables
    query_tpl = textwrap.dedent(
        """\
        CREATE TABLE IF NOT EXISTS {newdb}.{table}
        WITH (
            format = 'ORC',
            write_compression = 'ZSTD',
            external_location = '{location}/{table}/'
        )
        AS SELECT * FROM {basedb}.{table}
        WHERE {field} IN (select hash from swhids)
        """
    )

    for table, join_field in get_tables_join_field("athena"):
        ctas_query = query_tpl.format(
            newdb=subdataset_db,
            basedb=dataset_db,
            location=subdataset_s3_path,
            table=table,
            field=join_field,
        )

        # Temporary fix: Athena no longer supports >32MB rows, but some of
        # the objects were added to the dataset before this restriction was
        # in place.
        if table in ("revision", "release"):
            ctas_query += " AND length(message) < 100000"

        query(
            athena_client,
            ctas_query,
            desc="Creating join table {}".format(table),
        )


def generate_subdataset_datafusion(
    dataset_path: Path,
    output_path: Path,
    swhids_file: Path,
):
    """Generate a subdataset by filtering local ORC files using DataFusion."""
    from datafusion import SessionContext
    import pyarrow
    import pyarrow.dataset as ds
    import pyarrow.orc as pa_orc

    def filter_and_write_table(table_name: str, query_str: str, ctx: SessionContext):
        """Execute query and write results to ORC file."""
        import tqdm

        df = ctx.sql(query_str)
        output_dir = output_path / table_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{table_name}.orc"

        writer = None
        with tqdm.tqdm(desc=f"Filtering {table_name}", unit="rows", unit_scale=True) as pbar:
            for batch in df:
                arrow_table = pyarrow.Table.from_batches([batch.to_pyarrow()])
                if writer is None:
                    writer = pa_orc.ORCWriter(str(output_file), compression="ZSTD")
                writer.write(arrow_table)
                pbar.update(arrow_table.num_rows)
        if writer is not None:
            writer.close()
        return writer is not None

    ctx = SessionContext()

    # Register SWHID table from colon-delimited file
    ctx.sql(
        f"""
        CREATE EXTERNAL TABLE swhids (
            swhprefix STRING,
            version INT,
            type STRING,
            hash STRING
        )
        STORED AS CSV
        OPTIONS ('format.has_header' 'false', 'format.delimiter' ':')
        LOCATION '{swhids_file}'
    """
    )

    # Register each source ORC table using pyarrow.dataset for lazy loading
    registered_tables = set()
    for table_name in TABLES:
        orc_dir = dataset_path / table_name
        if not orc_dir.is_dir():
            continue
        dataset = ds.dataset(orc_dir, format=ds.OrcFileFormat())
        ctx.register_dataset(table_name, dataset)
        registered_tables.add(table_name)

    # First, filter the origin table and register it for joining
    # This way we only compute SHA1 once (via the id field) instead of three times
    filtered_origin_registered = False
    if "origin" in registered_tables:
        origin_query = """
            SELECT * FROM origin
            WHERE id IN (SELECT hash FROM swhids)
        """
        has_data = filter_and_write_table("origin", origin_query, ctx)
        if has_data:
            # Register the filtered origin table for joins
            filtered_dataset = ds.dataset(
                output_path / "origin", format=ds.OrcFileFormat()
            )
            ctx.register_dataset("filtered_origin", filtered_dataset)
            filtered_origin_registered = True

    # Filter remaining tables
    for table_name, join_field in get_tables_join_field("datafusion"):
        if table_name not in registered_tables:
            continue
        # Skip origin since we already processed it
        if table_name == "origin":
            continue

        # For origin_visit and origin_visit_status, join with filtered origins
        if table_name in ("origin_visit", "origin_visit_status") and filtered_origin_registered:
            query_str = f"""
                SELECT t.* FROM {table_name} t
                INNER JOIN filtered_origin fo ON t.origin = fo.url
            """
        else:
            query_str = f"""
                SELECT * FROM {table_name}
                WHERE {join_field} IN (SELECT hash FROM swhids)
            """

        if table_name in ("revision", "release"):
            # message is binary type, use length() for binary data
            query_str += " AND length(message) < 100000"

        filter_and_write_table(table_name, query_str, ctx)
