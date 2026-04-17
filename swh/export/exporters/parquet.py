# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from importlib.metadata import version
import logging
from typing import Any, Callable, Optional, Type
import uuid

import pyarrow
import pyarrow.parquet

from ..relational import BLOOM_FILTER_COLUMNS, TABLES
from .tabular import BaseTabularExporter, BaseWriter

ObjNotFoundError: Type[Exception]
get_objstorage: Optional[Callable]
try:
    from swh.objstorage.factory import get_objstorage
    from swh.objstorage.objstorage import ObjNotFoundError
except ImportError:
    get_objstorage = None
    ObjNotFoundError = Exception  # helps keep mypy happy


logger = logging.getLogger(__name__)

ARROW_TYPE_MAP = {
    "string": pyarrow.string(),
    "smallint": pyarrow.int16(),
    "int": pyarrow.int32(),
    "bigint": pyarrow.int64(),
    # a guaranteed "reasonable" timestamp (minted by a SWH loader)
    "visit_timestamp": pyarrow.timestamp("us", tz="UTC"),
    # a possibly wild timestamp (seen in the wild)
    "timestamp": pyarrow.struct(
        [
            ("seconds", pyarrow.int64()),
            ("nanoseconds", pyarrow.uint32()),
        ]
    ),
    "binary": pyarrow.binary(),
}

EXPORT_SCHEMA = {
    table_name: pyarrow.schema(
        [
            pyarrow.field(column_name, ARROW_TYPE_MAP[column_type])
            for column_name, column_type in columns
        ]
    )
    for table_name, columns in TABLES.items()
}


class ParquetWriter(BaseWriter):
    def __init__(
        self,
        schema: pyarrow.Schema,
        writer: pyarrow.parquet.ParquetWriter,
        table_name: str,
        unique_id: uuid.UUID,
    ):
        writer.add_key_value_metadata(
            dict(
                swh_object_type=table_name,
                swh_uuid=str(unique_id),
                swh_model_version=version("swh.model"),
                swh_export_version=version("swh.export"),
                # maybe put a copy of the config (redacted) also?
            )
        )
        self.schema = schema
        self.writer = writer

        self.buffers: list[list[Any]] = [[] for _ in self.schema.names]
        self._current_row = 0

    def current_row(self) -> int:
        return self._current_row

    def write(self, row: tuple) -> None:
        assert len(row) == len(self.buffers)
        for buf, cell in zip(self.buffers, row):
            buf.append(cell)

        self._current_row += 1

        if len(buf) > 1024:
            self.flush()

    def flush(self) -> None:
        batch = pyarrow.RecordBatch.from_pydict(
            dict(zip(self.schema.names, self.buffers)), schema=self.schema
        )
        self.write_batch(batch)
        for buf in self.buffers:
            buf.clear()

    def write_batch(self, batch: pyarrow.RecordBatch) -> None:
        self.writer.write(batch)

    def close(self) -> None:
        self.flush()
        self.writer.close()

    def __del__(self):
        if self.buffers[0]:
            raise ValueError("ParquetWriter deleted without flushing")


class ParquetExporter(BaseTabularExporter[ParquetWriter]):
    """
    Implementation of an exporter which writes the entire graph dataset as
    Parquet files. Useful for large scale processing, notably on cloud instances
    (e.g BigQuery, Amazon Athena, Azure).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        config = self.config.get("parquet", {})
        self.max_rows = config.get("max_rows", {})
        self.with_data = config.get("with_data", False)
        self.remove_pull_requests = config.get("remove_pull_requests", False)

        self._init_arrow_exporter(config)

    def new_writer_for(self, table_name: str, unique_id: uuid.UUID) -> ParquetWriter:
        object_type_dir = self.export_path / table_name
        path = object_type_dir / (f"{table_name}-{unique_id}.parquet")

        schema = EXPORT_SCHEMA[table_name]
        writer = pyarrow.parquet.ParquetWriter(
            path,
            schema,
            compression="zstd",
            bloom_filter_options={col: True for col in BLOOM_FILTER_COLUMNS},
        )
        return ParquetWriter(schema, writer, table_name, unique_id)

    def new_person_writer(self, unique_id: uuid.UUID) -> ParquetWriter:
        table_name = "person"
        object_type_dir = self.export_path / table_name
        path = object_type_dir / (f"{table_name}-{unique_id}.parquet")

        schema = pyarrow.schema(
            [
                pyarrow.field("fullname", pyarrow.binary()),
                pyarrow.field("sha256_fullname", pyarrow.binary()),
            ]
        )
        writer = pyarrow.parquet.ParquetWriter(
            path,
            schema,
            compression="zstd",
            bloom_filter_options={col: True for col in BLOOM_FILTER_COLUMNS},
        )
        return ParquetWriter(schema, writer, table_name, unique_id)
