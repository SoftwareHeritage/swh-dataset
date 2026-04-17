# Copyright (C) 2020-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from importlib.metadata import version
import logging
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple, Type, Union
import uuid

from pyorc import (
    BigInt,
    Binary,
    CompressionKind,
    Int,
    SmallInt,
    String,
    Struct,
    Timestamp,
    TypeKind,
    Writer,
)
from pyorc.converters import ORCConverter

from ..relational import BLOOM_FILTER_COLUMNS, TABLES
from .tabular import BaseTabularExporter, BaseWriter, datetime_to_tuple

ObjNotFoundError: Type[Exception]
get_objstorage: Optional[Callable]
try:
    from swh.objstorage.factory import get_objstorage
    from swh.objstorage.objstorage import ObjNotFoundError
except ImportError:
    get_objstorage = None
    ObjNotFoundError = Exception  # helps keep mypy happy


logger = logging.getLogger(__name__)


ORC_TYPE_MAP = {
    "string": String,
    "smallint": SmallInt,
    "int": Int,
    "bigint": BigInt,
    "visit_timestamp": Timestamp,
    "timestamp": Timestamp,
    "binary": Binary,
}

EXPORT_SCHEMA = {
    table_name: Struct(
        **{
            column_name: ORC_TYPE_MAP[column_type]()
            for column_name, column_type in columns
        }
    )
    for table_name, columns in TABLES.items()
}


class SWHTimestampConverter(ORCConverter):
    """This is an ORCConverter compatible class to convert timestamps from/to ORC files

    timestamps in python are given as a couple (seconds, microseconds) and are
    serialized as a couple (seconds, nanoseconds) in the ORC file.

    Reimplemented because we do not want the Python object to be converted as
    ORC timestamp to be Python datatime objects, since swh.model's Timestamp
    cannot be converted without loss a Python datetime objects.
    """

    # use Any as timezone annotation to make it easier to run mypy on python <
    # 3.9, plus we do not use the timezone argument here...
    @staticmethod
    def from_orc(
        seconds: int,
        nanoseconds: int,
        timezone: Any,
    ) -> Tuple[int, int]:
        return (seconds, nanoseconds // 1000)

    @staticmethod
    def to_orc(
        obj: Optional[Union[datetime.datetime, Tuple[int, int]]],
        timezone: Any,
    ) -> Optional[Tuple[int, Optional[int]]]:
        if obj is None:
            return None
        if type(obj) is datetime.datetime:
            t = datetime_to_tuple(obj)
            assert t is not None
            seconds, microseconds = t
            return (seconds, microseconds * 1000)
        else:
            if TYPE_CHECKING:
                assert not isinstance(obj, datetime.datetime)
            return (obj[0], obj[1] * 1000 if obj[1] is not None else None)


class ORCWriter(BaseWriter):
    def __init__(self, writer: Writer, file, table_name: str, unique_id: uuid.UUID):

        writer.set_user_metadata(
            swh_object_type=table_name.encode(),
            swh_uuid=str(unique_id).encode(),
            swh_model_version=version("swh.model").encode(),
            swh_export_version=version("swh.export").encode(),
            # maybe put a copy of the config (redacted) also?
        )

        self.writer = writer
        self.file = file

    def current_row(self) -> int:
        return self.writer.current_row

    def write(self, row: tuple) -> None:
        self.writer.write(row)

    def close(self) -> None:
        self.writer.close()
        self.file.close()


class ORCExporter(BaseTabularExporter[ORCWriter]):
    """
    Implementation of an exporter which writes the entire graph dataset as
    ORC files. Useful for large scale processing, notably on cloud instances
    (e.g BigQuery, Amazon Athena, Azure).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        config = self.config.get("orc", {})
        self.max_rows = config.get("max_rows", {})
        self.with_data = config.get("with_data", False)
        self.remove_pull_requests = config.get("remove_pull_requests", False)

        self._init_arrow_exporter(config)

    def new_writer_for(self, table_name: str, unique_id: uuid.UUID) -> ORCWriter:
        object_type_dir = self.export_path / table_name
        path = object_type_dir / (f"{table_name}-{unique_id}.orc")

        file = path.open("wb")
        writer = Writer(
            file,
            EXPORT_SCHEMA[table_name],
            compression=CompressionKind.ZSTD,
            converters={
                TypeKind.TIMESTAMP: SWHTimestampConverter,
            },
            bloom_filter_columns=BLOOM_FILTER_COLUMNS[table_name],
        )

        return ORCWriter(writer, file, table_name, unique_id)

    def new_person_writer(self, unique_id: uuid.UUID) -> ORCWriter:
        table_name = "person"
        object_type_dir = self.export_path / table_name
        path = object_type_dir / (f"{table_name}-{unique_id}.orc")

        schema = Struct(fullname=Binary(), sha256_fullname=Binary())
        file = path.open("wb")
        writer = Writer(
            file,
            schema,
            compression=CompressionKind.ZSTD,
            bloom_filter_columns=[0, 1],
        )

        return ORCWriter(writer, file, table_name, unique_id)
