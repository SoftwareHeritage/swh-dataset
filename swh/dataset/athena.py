# Copyright (C) 2021  The Software Heritage developers
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

import boto3
import botocore.exceptions

from swh.dataset.relational import TABLES


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
    remotely by using the relational schemas in swh.dataset.relational.
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
    tables_join_field = [
        ("origin", "lower(to_hex(sha1(to_utf8(url))))"),
        ("origin_visit", "lower(to_hex(sha1(to_utf8(origin))))"),
        ("origin_visit_status", "lower(to_hex(sha1(to_utf8(origin))))"),
        ("snapshot", "id"),
        ("snapshot_branch", "snapshot_id"),
        ("release", "id"),
        ("revision", "id"),
        ("revision_history", "id"),
        ("directory", "id"),
        ("directory_entry", "directory_id"),
        ("content", "sha1_git"),
        ("skipped_content", "sha1_git"),
    ]

    for table, join_field in tables_join_field:
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
        if table == "revision":
            ctas_query += " AND length(message) < 100000"

        query(
            athena_client,
            ctas_query,
            desc="Creating join table {}".format(table),
        )
