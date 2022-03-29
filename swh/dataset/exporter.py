# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import pathlib
from types import TracebackType
from typing import Any, Dict, Optional, Type
import uuid


class Exporter:
    """
    Base class for all the exporters.

    Each export can have multiple exporters, so we can read the journal a single
    time, then export the objects we read in different formats without having to
    re-read them every time.

    Override this class with the behavior for an export in a specific export
    format. You have to overwrite process_object() to make it write to the
    appropriate export files.

    You can also put setup and teardown logic in __enter__ and __exit__, and it
    will be called automatically.
    """

    def __init__(
        self, config: Dict[str, Any], export_path, *args: Any, **kwargs: Any
    ) -> None:
        self.config: Dict[str, Any] = config
        self.export_path = pathlib.Path(export_path)
        self.exit_stack = contextlib.ExitStack()

    def __enter__(self) -> "Exporter":
        self.export_path.mkdir(exist_ok=True, parents=True)
        self.exit_stack.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return self.exit_stack.__exit__(exc_type, exc_value, traceback)

    def process_object(self, object_type: str, obj: Dict[str, Any]) -> None:
        """
        Process a SWH object to export.

        Override this with your custom exporter.
        """
        raise NotImplementedError

    def get_unique_file_id(self) -> str:
        """
        Return a unique random file id for the current process.

        If config['test_unique_file_id'] is set, it will be used instead.
        """
        return str(self.config.get("test_unique_file_id", uuid.uuid4()))


class ExporterDispatch(Exporter):
    """
    Like Exporter, but dispatches each object type to a different function
    (e.g you can override `process_origin(self, object)` to process origins.)
    """

    def process_object(self, object_type: str, obj: Dict[str, Any]) -> None:
        method_name = "process_" + object_type
        if hasattr(self, method_name):
            getattr(self, method_name)(obj)
