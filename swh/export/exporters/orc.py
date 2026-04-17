# Copyright (C) 2020-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
import hashlib
from importlib.metadata import version
import logging
import math
from types import TracebackType
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union, cast
import uuid

import pyarrow.orc

from ..exporter import ExporterDispatch
from ..relational import BLOOM_FILTER_COLUMNS, MAIN_TABLES, TABLES
from ..utils import remove_pull_requests, subdirectories_for_object_type
from .arrow import EXPORT_SCHEMA, BaseArrowExporter

ObjNotFoundError: Type[Exception]
get_objstorage: Optional[Callable]
try:
    from swh.objstorage.factory import get_objstorage
    from swh.objstorage.objstorage import ObjNotFoundError
except ImportError:
    get_objstorage = None
    ObjNotFoundError = Exception  # helps keep mypy happy


logger = logging.getLogger(__name__)


def swh_date_to_tuple(
    obj: Optional[TimestampWithTimezone],
) -> Union[Tuple[None, None, None], Tuple[Tuple[int, int], int, bytes]]:
    if obj is None or obj.timestamp is None:
        return (None, None, None)
    return (
        (obj.timestamp.seconds, obj.timestamp.microseconds),
        obj.offset_minutes(),
        obj.offset_bytes,
    )


def datetime_to_tuple(obj: Optional[datetime]) -> Optional[Tuple[int, int]]:
    if obj is None:
        return None
    return (math.floor(obj.timestamp()), obj.microsecond)


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
        obj: Optional[Tuple[int, int]],
        timezone: Any,
    ) -> Optional[Tuple[int, Optional[int]]]:
        if obj is None:
            return None
        return (obj[0], obj[1] * 1000 if obj[1] is not None else None)


class ORCWriter(BaseWriter):
    def __init__(self, columns: dict[str, Any], writer: pyarrow.orc.ORCWriter):
        super().__init__(columns)
        self.writer = writer

    def write_batch(self, batch: pyarrow.RecordBatch) -> None:
        self.writer.write(batch)

    def close(self) -> None:
        self.flush()
        self.writer.close()


class ORCExporter(BaseArrowExporter[ORCWriter]):
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

        self._init_arrow_exporter()

    def new_writer_for(self, table_name: str, unique_id: uuid.UUID) -> ORCWriter:
        object_type_dir = self.export_path / table_name

        export_file = object_type_dir / (f"{table_name}-{unique_id}.orc")
        self.writer_files[table_name] = export_obj
        writer = pyarrow.orc.ORCWriter(
            export_file,
            compression=CompressionKind.ZSTD,
            bloom_filter_columns=BLOOM_FILTER_COLUMNS[table_name],
        )

        writer.set_user_metadata(
            swh_object_type=table_name.encode(),
            swh_uuid=unique_id.encode(),
            swh_model_version=version("swh.model").encode(),
            swh_export_version=version("swh.export").encode(),
            # maybe put a copy of the config (redacted) also?
        )

        return ORCWriter(EXPORT_SCHEMA[table_name], writer)
