# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from types import TracebackType
from typing import Any, Dict, Optional, Type


class Exporter:
    def __init__(self, config: Dict[str, Any], *args: Any, **kwargs: Any) -> None:
        self.config: Dict[str, Any] = config

    def __enter__(self) -> "Exporter":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        pass

    def process_object(self, object_type: str, object: Dict[str, Any]) -> None:
        """
        Process a SWH object to export.

        Override this with your custom worker.
        """
        raise NotImplementedError


class ExporterDispatch(Exporter):
    """
    Like Exporter, but dispatches each object type to a different function.
    """

    def process_object(self, object_type: str, object: Dict[str, Any]) -> None:
        method_name = "process_" + object_type
        if hasattr(self, method_name):
            getattr(self, method_name)(object)
