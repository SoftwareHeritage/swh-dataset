# Copyright (C) 2022-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks
===========

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks,
as an alternative to the CLI that can be composed with other tasks,
such as swh-graph's.

File layout
-----------

Tasks in this module work on "export directories", which have this layout::

    swh_<date>[_<flavor>]/
        edges/
            origin/
            snapshot/
            ...
            stamps/
                origin
                snapshot
                ...
        orc/
            origin/
            snapshot/
            ...
            stamps/
                origin
                snapshot
                ...
        meta/
            export.json

``stamps`` files are written after corresponding directories are written.
Their presence indicates the corresponding directory was fully generated/copied.
This allows skipping work that was already done, while ignoring interrupted jobs.
They are omitted after the initial export (ie. when downloading to/from other machines).

``meta/export.json`` contains information about the dataset, for provenance tracking.
For example:

.. code-block:: json

    {
        "flavor": "full",
        "export_start": "2022-11-08T11:00:54.998799+00:00",
        "export_end": "2022-11-08T11:05:53.105519+00:00",
        "brokers": [
            "broker1.journal.staging.swh.network:9093"
        ],
        "prefix": "swh.journal.objects",
        "formats": [
            "edges",
            "orc"
        ],
        "object_types": [
            "revision",
            "release",
            "snapshot",
            "origin_visit_status",
            "origin_visit",
            "origin"
        ],
        "privileged": false,
        "hostname": "desktop5",
        "tool": {
            "name": "swh.export",
            "version": "0.3.2"
        }
    }

``object_types`` contains a list of "main tables" exported; this excludes relational
tables like ``directory_entry``.

Running all on staging
----------------------

An easy way to run it (eg. on the staging database), is to have these config
files:

.. code-block: yaml
    :caption: graph.staging.yml

    journal:
      brokers:
        - broker1.journal.staging.swh.network:9093
      prefix: swh.journal.objects
      sasl.mechanism: "SCRAM-SHA-512"
      security.protocol: "sasl_ssl"
      sasl.username: "<username>"
      sasl.password: "<password>"
      privileged: false
      group_id: "<username>-test-dataset-export"

.. code-block: yaml
    :caption: luigi.cfg

    [ExportGraph]
    config=graph.staging.yml
    processes=16

    [RunExportAll]
    formats=edges,orc
    s3_athena_output_location=s3://vlorentz-test2/tmp/athena-output/

And run this command, for example::

    luigi --log-level INFO --local-scheduler --module swh.export.luigi RunExportAll \
            --UploadExportToS3-local-export-path=/poolswh/softwareheritage/2022-11-09_staging/ \
            --s3-export-path=s3://vlorentz-test2/vlorentz_2022-11-09_staging/ \
            --athena-db-name=vlorentz_20221109_staging

Note that this arbitrarily divides config options between :file:`luigi.cfg` and the CLI
for readability; but `they can be used interchangeably <https://luigi.readthedocs.io/en/stable/configuration.html#parameters-from-config-ingestion>`__
"""  # noqa

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import enum
import logging
from pathlib import Path
import shutil
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Hashable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import luigi

from swh.export import cli
from swh.export.relational import MAIN_TABLES
from swh.export.utils import subdirectories_for_object_type

if TYPE_CHECKING:
    from swh.model.swhids import ExtendedSWHID

ObjectType = enum.Enum(  # type: ignore[misc]
    "ObjectType", [obj_type for obj_type in MAIN_TABLES.keys()]
)
Format = enum.Enum("Format", list(cli.AVAILABLE_EXPORTERS))  # type: ignore[misc]


T = TypeVar("T", bound=Hashable)


def merge_lists(lists: Iterator[List[T]]) -> List[T]:
    """Returns a list made of all items of the arguments, with no duplicate."""
    res = set()
    for list_ in lists:
        res.update(set(list_))
    return list(res)


class PathParameter(luigi.PathParameter):
    """
    A parameter that is a local filesystem path.

    If ``is_dir``, ``is_file``, or ``exists`` is :const:`True`, then existence of
    the path (and optionally type) is checked.

    If ``create`` is set, then ``is_dir`` must be :const:`True`, and the directory
    is created if it does not already exist.
    """

    def __init__(
        self,
        is_dir: bool = False,
        is_file: bool = False,
        exists: bool = False,
        create: bool = False,
        **kwargs,
    ):
        """
        :param is_dir: whether the path should be to a directory
        :param is_file: whether the path should be to a directory
        :param exists: whether the path should already exist
        :param create: whether the path should be created if it does not exist

        ``is_dir`` and ``is_file`` are mutually exclusive.
        ``exists`` and ``create`` are mutually exclusive.
        """
        if create and not is_dir:
            raise ValueError("`is_dir` must be True if `create` is True")
        if is_dir and is_file:
            raise ValueError("`is_dir` and `is_file` are mutually exclusive")

        super().__init__(**kwargs)

        self.is_dir = is_dir
        self.is_file = is_file
        self.exists = exists
        self.create = create

    def parse(self, s: str) -> Path:
        path = Path(s)

        if self.create:
            path.mkdir(parents=True, exist_ok=True)

        if (self.exists or self.is_dir or self.is_file) and not path.exists():
            raise ValueError(f"{s} does not exist")
        if self.is_dir and not path.is_dir():
            raise ValueError(f"{s} is not a directory")
        if self.is_file and not path.is_file():
            raise ValueError(f"{s} is not a file")

        return path


class S3PathParameter(luigi.Parameter):
    """A parameter that strip trailing slashes"""

    def __init__(self, *args, **kwargs):
        """"""
        # Override luigi.Parameter.__init__'s docstring, which contains a broken ref
        super().__init__(*args, **kwargs)

    def normalize(self, s):
        return s.rstrip("/")


class FractionalFloatParameter(luigi.FloatParameter):
    """A float parameter that must be between 0 and 1"""

    def __init__(self, *args, **kwargs):
        """"""
        # Override luigi.Parameter.__init__'s docstring, which contains a broken ref
        super().__init__(*args, **kwargs)

    def parse(self, s):
        v = super().parse(s)

        if not 0.0 <= v <= 1.0:
            raise ValueError(f"{s} is not a float between 0 and 1")

        return v


def stamps_paths(formats: List[Format], object_types: List[ObjectType]) -> List[str]:
    """Returns a list of (local FS or S3) paths used to mark tables as successfully
    exported.
    """
    return [
        f"tmp/stamps/{object_type.name.lower()}"
        for format_ in formats
        for object_type in object_types
    ]


def _export_metadata_has_object_types(
    export_metadata: Union[luigi.LocalTarget, "luigi.contrib.s3.S3Target"],
    object_types: List[ObjectType],
) -> bool:
    import json

    with export_metadata.open() as fd:
        meta = json.load(fd)
    return set(meta["object_types"]) >= {
        object_type.name for object_type in object_types
    }


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


class StartExport(luigi.Task):
    """Pseudo-task that computes the journal offsets from and to which objects should
    be exported"""

    config_file: Path = PathParameter(is_file=True)  # type: ignore[assignment]
    local_export_path: Path = PathParameter(is_dir=True, create=True)  # type: ignore[assignment]
    local_sensitive_export_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    export_id = luigi.OptionalParameter(
        description="""
        Unique ID of the export run. This is appended to the kafka
        group_id config file option. If group_id is not set in the
        'journal' section of the config file, defaults to 'swh-export-export-'.
        """,
    )
    margin: float = FractionalFloatParameter(  # type: ignore[assignment]
        default=1.0,
        description="""
        Offset margin to start consuming from. E.g. is set to '0.95',
        consumers will start at 95%% of the last committed offset;
        in other words, start earlier than last committed position.
        """,
    )
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )

    def output(self) -> Dict[Union[str, ObjectType], luigi.LocalTarget]:
        """Returns a stamp file for each step, in `self.local_export_path/tmp/stamps/`"""
        results: Dict[Union[str, ObjectType], luigi.LocalTarget] = {
            "stamp": luigi.LocalTarget(
                self.local_export_path / "tmp" / "stamps" / "START.json"
            )
        }

        offsets_dir = self.local_export_path / "tmp" / "offsets"
        offsets_dir.mkdir(exist_ok=True, parents=True)
        results.update(
            {
                obj_type: luigi.LocalTarget(offsets_dir / f"{obj_type.name}.json")
                for obj_type in self.object_types
            }
        )
        return results

    def complete(self) -> bool:
        import json

        if not super().complete():
            return False

        with self.output()["stamp"].open() as f:
            export_id = json.load(f)["export_id"]
        assert export_id == self.export_id, (
            f"Export was started with export_id {export_id} "
            f"but the current task is for {self.export_id}"
        )

        return True

    def run(self) -> None:
        """Writes the offsets file"""
        import datetime
        import json

        import yaml

        from .journalprocessor import ParallelJournalProcessor

        with open(self.config_file) as f:
            config = yaml.safe_load(f)

        logger = logging.getLogger(__name__)

        masked_swhids = get_masked_swhids(logger, config)

        # {obj_type: {partition: (low, high)}
        offsets: Dict[str, Dict[int, Tuple[int, int]]] = {}
        for obj_type in [  # order matter, in order to avoid holes
            ObjectType.origin_visit_status,  # type: ignore[attr-defined]
            ObjectType.origin_visit,  # type: ignore[attr-defined]
            ObjectType.origin,  # type: ignore[attr-defined]
            ObjectType.snapshot,  # type: ignore[attr-defined]
            ObjectType.release,  # type: ignore[attr-defined]
            ObjectType.revision,  # type: ignore[attr-defined]
            ObjectType.directory,  # type: ignore[attr-defined]
            ObjectType.skipped_content,  # type: ignore[attr-defined]
            ObjectType.content,  # type: ignore[attr-defined]
        ]:
            if obj_type not in self.object_types:
                continue
            journal_processor = ParallelJournalProcessor(
                config,
                masked_swhids,
                [],  # exporters, not needed yet
                self.export_id,
                obj_type.name,
                node_sets_path=self.local_export_path / ".node_sets",
                persons_dir=self.local_export_path / "unused",  # placeholder
                processes=4,  # very quick, no need for more
            )
            journal_processor.get_offsets()
            assert journal_processor.offsets is not None
            offsets[obj_type] = journal_processor.offsets

        (self.local_export_path / "tmp" / "dup_persons").mkdir(
            parents=True, exist_ok=True
        )

        for obj_type in self.object_types:
            with self.output()[obj_type].open("w") as f:
                json.dump({"offsets": offsets[obj_type]}, f)
        with self.output()["stamp"].open("w") as f:
            json.dump(
                {
                    "export_id": self.export_id,
                    "start_date": datetime.datetime.now(
                        tz=datetime.timezone.utc
                    ).isoformat(),
                    "margin": self.margin,
                    "object_types": [obj_type.name for obj_type in self.object_types],
                },
                f,
            )


class ExportTopic(luigi.Task):
    """Exports a single topic, given already computed offsets in the journal."""

    config_file: Path = PathParameter(is_file=True)  # type: ignore[assignment]
    local_export_path: Path = PathParameter(is_dir=True, create=True)  # type: ignore[assignment]
    local_sensitive_export_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    export_id = luigi.OptionalParameter(
        description="""
        Unique ID of the export run. This is appended to the kafka
        group_id config file option. If group_id is not set in the
        'journal' section of the config file, defaults to 'swh-export-export-'.
        """,
    )
    formats = luigi.EnumListParameter(enum=Format, batch_method=merge_lists)
    processes = luigi.IntParameter(default=1, significant=False)
    margin: float = FractionalFloatParameter(  # type: ignore[assignment]
        default=1.0,
        description="""
        Offset margin to start consuming from. E.g. is set to '0.95',
        consumers will start at 95%% of the last committed offset;
        in other words, start earlier than last committed position.
        """,
    )
    object_types = luigi.EnumListParameter(enum=ObjectType, default=list(ObjectType))

    def _stamp_files(self) -> List[Path]:
        stamp_dir = Path(self.local_export_path) / "tmp" / "stamps"
        return [stamp_dir / f"{obj_type.name}.json" for obj_type in self.object_types]

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`StartExport`"""
        return {
            "start": StartExport(
                config_file=self.config_file,
                local_export_path=self.local_export_path,
                local_sensitive_export_path=self.local_sensitive_export_path,
                export_id=self.export_id,
                margin=self.margin,
                object_types=self.object_types,
            )
        }

    def output(self) -> List[luigi.LocalTarget]:
        """Returns a :class:`luigi.LocalTarget` instance for each stamp file"""
        return list(map(luigi.LocalTarget, self._stamp_files()))

    def _setrlimit(self, nb_shards):
        import resource

        logger = logging.getLogger(__name__)

        # ParallelJournalProcessor opens 256 LevelDBs in total. Depending on the number of
        # processes, this can exceed the maximum number of file descriptors (soft limit
        # defaults to 1024 on Debian), so let's increase it.
        (soft, hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
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

    def run(self) -> None:
        """Consumes all of the ``self.OBJECT_TYPE`` topic into
        ``self.export_path / self.OBJECT_TYPE``."""
        import functools
        from importlib import import_module
        import json
        import shutil

        import yaml

        from .journalprocessor import ParallelJournalProcessor

        with open(self.config_file) as f:
            config = yaml.safe_load(f)
        logger = logging.getLogger(__name__)

        masked_swhids = get_masked_swhids(logger, config)

        offsets: Dict[ObjectType, Dict[int, Tuple[int, int]]] = {}
        for obj_type in self.object_types:
            with self.input()["start"][obj_type].open("r") as f:
                # {obj_type: {partition: (low, high)}
                offsets[obj_type] = {
                    int(partition): (low, high)
                    for (partition, (low, high)) in json.load(f)["offsets"].items()
                }

        print(list(offsets))

        self._setrlimit(
            sum(
                len(topic_offsets)
                for (obj_type, topic_offsets) in offsets.items()
                if obj_type in self.object_types
            )
        )

        def importcls(clspath):
            mod, cls = clspath.split(":")
            m = import_module(mod)
            return getattr(m, cls)

        exporter_cls = dict(
            (fmt, importcls(clspath))
            for (fmt, clspath) in cli.AVAILABLE_EXPORTERS.items()
            if Format[fmt] in self.formats
        )

        parallel_exporters = {}
        for obj_type in self.object_types:
            subdirectories = subdirectories_for_object_type(obj_type.name.lower())

            for f in self.formats:
                for subdirectory in subdirectories:
                    export_directory = self.local_export_path / f.name / subdirectory
                    try:
                        # remove any leftover from a failed previous run
                        shutil.rmtree(export_directory)
                    except FileNotFoundError:
                        pass
                    # ensure export directory exists as it is expected by the graph compression
                    # tool but it will not be created if the journal topic to export is empty
                    export_directory.mkdir(parents=True)
                if self.local_sensitive_export_path is not None:
                    try:
                        shutil.rmtree(
                            self.local_sensitive_export_path / f.name / obj_type.name
                        )
                    except FileNotFoundError:
                        pass

            exporters = [
                functools.partial(
                    exporter_cls[f.name],
                    config=config,
                    object_types=[obj_type.name],
                    export_path=self.local_export_path / f.name,
                    sensitive_export_path=(
                        self.local_sensitive_export_path / f.name
                        if self.local_sensitive_export_path is not None
                        else None
                    ),
                )
                for f in self.formats
            ]
            journal_processor = ParallelJournalProcessor(
                config,
                masked_swhids,
                exporters,
                self.export_id,
                obj_type.name,
                node_sets_path=self.local_export_path / ".node_sets",
                persons_dir=self.local_export_path / "tmp" / "dup_persons",
                processes=self.processes,
            )
            journal_processor.offsets = offsets[obj_type]
            parallel_exporters[obj_type] = journal_processor

        for obj_type, parallel_exporter in parallel_exporters.items():
            parallel_exporter.run()

        for obj_type in self.object_types:
            try:
                shutil.rmtree(self.local_export_path / ".node_sets" / obj_type.name)
            except FileNotFoundError:
                pass

        for path in self._stamp_files():
            path.write_text(json.dumps({}))


class ExportPersonsTable(luigi.Task):
    """Aggregates lists of persons exported by :class:`ExportTopic` into a single table
    with no duplicates."""

    config_file: Path = PathParameter(is_file=True)  # type: ignore[assignment]
    local_export_path: Path = PathParameter(is_dir=True, create=True)  # type: ignore[assignment]
    local_sensitive_export_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    export_id = luigi.OptionalParameter(
        description="""
        Unique ID of the export run. This is appended to the kafka
        group_id config file option. If group_id is not set in the
        'journal' section of the config file, defaults to 'swh-export-export-'.
        """,
    )
    formats = luigi.EnumListParameter(enum=Format, batch_method=merge_lists)
    processes = luigi.IntParameter(default=1, significant=False)
    margin: float = FractionalFloatParameter(  # type: ignore[assignment]
        default=1.0,
        description="""
        Offset margin to start consuming from. E.g. is set to '0.95',
        consumers will start at 95%% of the last committed offset;
        in other words, start earlier than last committed position.
        """,
    )
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`StartExport`, and an instance of :class:`ExportTopic`
        for each value in ``self.object_types``"""
        requirements: Dict[str, luigi.Task] = {
            "start": StartExport(
                config_file=self.config_file,
                local_export_path=self.local_export_path,
                local_sensitive_export_path=self.local_sensitive_export_path,
                export_id=self.export_id,
                margin=self.margin,
                object_types=self.object_types,
            )
        }
        requirements.update(
            {
                obj_type: ExportTopic(
                    config_file=self.config_file,
                    local_export_path=self.local_export_path,
                    local_sensitive_export_path=self.local_sensitive_export_path,
                    export_id=self.export_id,
                    formats=self.formats,
                    processes=self.processes,
                    margin=self.margin,
                    object_types=[obj_type],
                )
                for obj_type in self.object_types
                if obj_type in (ObjectType.revision, ObjectType.release)  # type: ignore[attr-defined]
            }
        )
        return requirements

    def output(self) -> Dict[str, luigi.LocalTarget]:
        """Returns ``{self.local_export_path}/tmp/stamps/person.json``"""
        return {
            "stamp": luigi.LocalTarget(
                Path(self.local_export_path) / "tmp" / "stamps" / "person.json"
            )
        }

    def run(self):
        """Aggregates lists of persons exported by :class:`ExportTopic` into a single
        table with no duplicates."""
        import json
        import uuid

        from .fullnames import process_fullnames

        if self.local_sensitive_export_path is not None:
            fullnames_export_path = self.local_sensitive_export_path / "orc" / "person"
            fullnames_export_path.mkdir(parents=True, exist_ok=True)
            fullnames_orc = fullnames_export_path / f"{uuid.uuid4()}.orc"
            process_fullnames(
                fullnames_orc, self.local_export_path / "tmp" / "dup_persons"
            )

        with self.output()["stamp"].open("w") as f:
            json.dump({}, f)


class ExportGraph(luigi.Task):
    """Exports the entire graph to the local filesystem.

    Example invocation::

        luigi --local-scheduler --module swh.export.luigi ExportGraph \
                --config=graph.prod.yml \
                --local-export-path=export/ \
                --formats=edges

    which is equivalent to this CLI call:

        swh export --config-file graph.prod.yml graph export export/ --formats=edges
    """

    config_file: Path = PathParameter(is_file=True)  # type: ignore[assignment]
    local_export_path: Path = PathParameter(is_dir=True, create=True)  # type: ignore[assignment]
    local_sensitive_export_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    export_id = luigi.OptionalParameter(
        default=None,
        description="""
        Unique ID of the export run. This is appended to the kafka
        group_id config file option. If group_id is not set in the
        'journal' section of the config file, defaults to 'swh-export-export-'.
        """,
    )
    formats = luigi.EnumListParameter(enum=Format, batch_method=merge_lists)
    processes = luigi.IntParameter(default=1, significant=False)
    margin: float = FractionalFloatParameter(  # type: ignore[assignment]
        default=1.0,
        description="""
        Offset margin to start consuming from. E.g. is set to '0.95',
        consumers will start at 95%% of the last committed offset;
        in other words, start earlier than last committed position.
        """,
    )
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )
    export_name = luigi.Parameter()

    def output(self) -> List[luigi.LocalTarget]:
        """Returns path of `meta/export.json` on the local FS."""
        return [self._meta()]

    def complete(self) -> bool:
        return super().complete() and _export_metadata_has_object_types(
            self._meta(), self.object_types
        )

    def _stamps(self):
        return [
            luigi.LocalTarget(self.local_export_path / path)
            for path in stamps_paths(self.formats, self.object_types)
        ]

    def _meta(self):
        return luigi.LocalTarget(self.local_export_path / "meta" / "export.json")

    def get_export_id(self) -> str:
        import json
        import logging
        import uuid

        logger = logging.getLogger(__name__)

        if self.export_id:
            logger.info("Using configured export id %s", self.export_id)
            return self.export_id
        else:
            start_stamp_path = self.local_export_path / "tmp" / "stamps" / "START.json"
            if start_stamp_path.exists():
                export_id = json.loads(start_stamp_path.read_text())["export_id"]
                logger.info("Reusing export id %s", export_id)
            else:
                export_id = f"{self.export_name}-{uuid.uuid4()}"
                logger.info("Creating new export with id %s", export_id)
            return export_id

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`StartExport`, and an instance of :class:`ExportTopic`
        for each value in ``self.object_types``"""
        export_id = self.get_export_id()
        kwargs = dict(
            config_file=self.config_file,
            local_export_path=self.local_export_path,
            local_sensitive_export_path=self.local_sensitive_export_path,
            export_id=export_id,
            margin=self.margin,
        )
        dependencies: Dict[str, luigi.Task] = {
            obj_type: ExportTopic(
                **kwargs,
                processes=self.processes,
                formats=self.formats,
                object_types=[obj_type],
            )
            for obj_type in self.object_types
        }
        dependencies["START"] = StartExport(
            **kwargs,
            object_types=self.object_types,
        )
        dependencies["PERSONS"] = ExportPersonsTable(
            **kwargs,
            formats=self.formats,
            object_types=self.object_types,
        )
        return dependencies

    def run(self) -> None:
        """Runs the full export, then writes stamps, then :file:`meta.json`."""
        import datetime
        from importlib.metadata import version
        import json
        import socket

        from swh.core import config

        conf = config.read(str(self.config_file))

        with self.input()["START"]["stamp"].open() as f:
            start_date = datetime.datetime.fromisoformat(json.load(f)["start_date"])
        end_date = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create stamps
        for output in self._stamps():
            output.makedirs()
            with output.open("w") as fd:
                pass

        # Write export metadata
        meta = {
            "flavor": "full",
            "export_start": start_date.isoformat(),
            "export_end": end_date.isoformat(),
            "brokers": conf["journal"]["brokers"],
            "prefix": conf["journal"]["prefix"],
            "formats": [format_.name for format_ in self.formats],
            "object_types": [object_type.name for object_type in self.object_types],
            "privileged": conf["journal"].get("privileged"),
            "hostname": socket.getfqdn(),
            "tool": {
                "name": "swh.export",
                "version": version("swh.export"),
            },
        }
        with self._meta().open("w") as fd:
            json.dump(meta, fd, indent=4)

        shutil.rmtree(self.local_export_path / "tmp")


class UploadExportToS3(luigi.Task):
    """Uploads a local dataset export to S3; creating automatically if it does
    not exist.

    Example invocation::

        luigi --local-scheduler --module swh.export.luigi UploadExportToS3 \
                --local-export-path=export/ \
                --formats=edges \
                --s3-export-path=s3://softwareheritage/graph/swh_2022-11-08
    """

    local_export_path: Path = PathParameter(is_dir=True, create=True, significant=False)  # type: ignore[assignment]
    formats = luigi.EnumListParameter(enum=Format, batch_method=merge_lists)
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )
    s3_export_path: str = S3PathParameter()  # type: ignore[assignment]

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`ExportGraph` task that writes local files at the
        expected location."""
        return [
            ExportGraph(
                local_export_path=self.local_export_path,
                formats=self.formats,
                object_types=self.object_types,
            )
        ]

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on S3."""
        return [self._meta()]

    def _meta(self):
        import luigi.contrib.s3

        return luigi.contrib.s3.S3Target(f"{self.s3_export_path}/meta/export.json")

    def complete(self) -> bool:
        """Returns whether the graph dataset was exported with a superset of
        ``object_types``"""
        if (
            "s3://softwareheritage/graph/"
            < self.s3_export_path
            < "s3://softwareheritage/graph/2022-12-07"
        ):
            # exports before 2022-12-07 did not have the metadata needed; skip check
            # for old exports.
            return True

        return super().complete() and _export_metadata_has_object_types(
            self._meta(), self.object_types
        )

    def run(self) -> None:
        """Copies all files: first the export itself, then :file:`meta.json`."""
        import os

        import luigi.contrib.s3
        import tqdm

        client = luigi.contrib.s3.S3Client()

        # recursively copy local files to S3, and end with stamps and export metadata
        for format_ in self.formats:
            for dirname in os.listdir(self.local_export_path / format_.name):
                if dirname in ("stamps", "tmp"):
                    # used as stamps while exporting, pointless to copy them
                    continue
                if dirname == "meta":
                    # used as final stamp; copy it at the end
                    continue
                local_dir = self.local_export_path / format_.name / dirname
                s3_dir = f"{self.s3_export_path}/{format_.name}/{dirname}"
                status_message = f"Uploading {format_.name}/{dirname}/"
                self.set_status_message(status_message)
                for file_ in tqdm.tqdm(
                    list(os.listdir(local_dir)),
                    desc=status_message,
                ):
                    local_path = local_dir / file_
                    s3_path = f"{s3_dir}/{file_}"
                    obj_summary = client.get_key(s3_path)
                    if (
                        obj_summary is not None
                        and obj_summary.size == local_path.stat().st_size
                    ):
                        # already uploaded (probably by a previous interrupted run)
                        continue
                    client.put_multipart(local_path, s3_path, ACL="public-read")

        client.put(
            self.local_export_path / "meta" / "export.json",
            self._meta().path,
            ACL="public-read",
        )


class DownloadExportFromS3(luigi.Task):
    """Downloads a local dataset export from S3.

    This performs the inverse operation of :class:`UploadExportToS3`

    Example invocation::

        luigi --local-scheduler --module swh.export.luigi DownloadExportFromS3 \
                --local-export-path=export/ \
                --formats=edges \
                --s3-export-path=s3://softwareheritage/graph/swh_2022-11-08
    """

    local_export_path: Path = PathParameter(is_dir=True, create=True)  # type: ignore[assignment]
    formats = luigi.EnumListParameter(enum=Format, batch_method=merge_lists)
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )
    s3_export_path: str = S3PathParameter(significant=False)  # type: ignore[assignment]
    parallelism = luigi.IntParameter(default=10, significant=False)

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`ExportGraph` task that writes local files at the
        expected location."""
        return [
            UploadExportToS3(
                local_export_path=self.local_export_path,
                formats=self.formats,
                object_types=self.object_types,
                s3_export_path=self.s3_export_path,
            )
        ]

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on the local filesystem."""
        return [self._meta()]

    def complete(self) -> bool:
        return super().complete() and _export_metadata_has_object_types(
            self._meta(), self.object_types
        )

    def _meta(self):
        return luigi.LocalTarget(self.local_export_path / "meta" / "export.json")

    def run(self) -> None:
        """Copies all files: first the export itself, then :file:`meta.json`."""
        import collections
        import multiprocessing.dummy

        import luigi.contrib.s3
        import tqdm

        client = luigi.contrib.s3.S3Client()

        # recursively copy local files to S3, and end with export metadata
        paths = []
        for format_ in self.formats:
            local_dir = self.local_export_path / format_.name
            s3_dir = f"{self.s3_export_path}/{format_.name}"
            files = list(client.list(s3_dir))
            assert files, "No files found"

            files_by_type = collections.defaultdict(list)
            for file in files:
                files_by_type[file.split("/")[0]].append(file)

            for object_type, files in files_by_type.items():
                (local_dir / object_type).mkdir(parents=True, exist_ok=True)
                for file_ in files:
                    paths.append(
                        (
                            f"{s3_dir}/{file_}",
                            str(local_dir / file_),
                        )
                    )

        with multiprocessing.dummy.Pool(self.parallelism) as p:
            for i, _ in tqdm.tqdm(
                enumerate(p.imap_unordered(lambda args: client.get(*args), paths)),
                total=len(paths),
                desc="Downloading graph export",
            ):
                self.set_progress_percentage(int(i * 100 / len(paths)))

        export_json_path = self.local_export_path / "meta" / "export.json"
        export_json_path.parent.mkdir(exist_ok=True)
        client.get(
            f"{self.s3_export_path}/meta/export.json",
            self._meta().path,
        )


class LocalExport(luigi.Task):
    """Task that depends on a local dataset being present -- either directly from
    :class:`ExportGraph` or via :class:`DownloadExportFromS3`.
    """

    local_export_path: Path = PathParameter(is_dir=True)  # type: ignore[assignment]
    local_sensitive_export_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    formats = luigi.EnumListParameter(enum=Format, batch_method=merge_lists)
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )
    export_task_type = luigi.TaskParameter(
        default=DownloadExportFromS3,
        significant=False,
        description="""The task used to get the dataset if it is not present.
        Should be either ``ExportGraph`` or ``DownloadExportFromS3``.""",
    )

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of either :class:`ExportGraph` or
        :class:`DownloadExportFromS3` depending on the value of
        :attr:`export_task_type`."""
        if issubclass(self.export_task_type, ExportGraph):
            return [
                ExportGraph(
                    local_export_path=self.local_export_path,
                    local_sensitive_export_path=self.local_sensitive_export_path,
                    formats=self.formats,
                    object_types=self.object_types,
                )
            ]
        elif issubclass(self.export_task_type, DownloadExportFromS3):
            return [
                DownloadExportFromS3(
                    local_export_path=self.local_export_path,
                    formats=self.formats,
                    object_types=self.object_types,
                )
            ]
        else:
            raise ValueError(
                f"Unexpected export_task_type: {self.export_task_type.__name__}"
            )

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on the local filesystem."""
        return [self._meta()]

    def _meta(self):
        return luigi.LocalTarget(self.local_export_path / "meta" / "export.json")

    def complete(self) -> bool:
        if "2020-" < self.local_export_path.name < "2022-12-07":
            # exports before 2022-12-07 did not have the metadata needed; skip check
            # for old exports.
            return True

        return super().complete() and _export_metadata_has_object_types(
            self._meta(), self.object_types
        )


class AthenaDatabaseTarget(luigi.Target):
    """Target for the existence of a database on Athena."""

    def __init__(self, name: str, table_names: Set[str]):
        self.name = name
        self.table_names = table_names

    def exists(self) -> bool:
        import boto3

        client = boto3.client("athena")

        database_list = client.list_databases(CatalogName="AwsDataCatalog")
        for database in database_list["DatabaseList"]:
            if database["Name"] == self.name:
                break
        else:
            # the database doesn't exist at all
            return False

        table_metadata = client.list_table_metadata(
            CatalogName="AwsDataCatalog", DatabaseName=self.name
        )
        missing_tables = self.table_names - {
            table["Name"] for table in table_metadata["TableMetadataList"]
        }
        return not missing_tables


class CreateAthena(luigi.Task):
    """Creates tables on AWS Athena pointing to a given graph dataset on S3.

    Example invocation::

        luigi --local-scheduler --module swh.export.luigi CreateAthena \
                --ExportGraph-config=graph.staging.yml \
                --athena-db-name=swh_20221108 \
                --object-types=origin,origin_visit \
                --s3-export-path=s3://softwareheritage/graph/swh_2022-11-08 \
                --s3-athena-output-location=s3://softwareheritage/graph/tmp/athena

    which is equivalent to this CLI call:

        swh export athena create \
                --database-name swh_20221108 \
                --location-prefix s3://softwareheritage/graph/swh_2022-11-08 \
                --output-location s3://softwareheritage/graph/tmp/athena \
                --replace-tables
    """

    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )
    s3_export_path = S3PathParameter()
    s3_athena_output_location = S3PathParameter()
    athena_db_name = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not self.s3_export_path.replace("-", "").endswith(
            "/" + self.athena_db_name.split("_", 1)[1]
        ):
            raise ValueError(
                f"S3 export path ({self.s3_export_path}) does not match "
                f"Athena database name ({self.athena_db_name})."
                f"They should use these formats: "
                f"'s3://<whatever>/YYYY-MM-DD[_<flavor>]/' "
                f"and '<prefix>_YYYYMMDD[_<flavor>]"
            )

    def requires(self) -> List[luigi.Task]:
        """Returns the corresponding :class:`UploadExportToS3` instance,
        with ORC as only format."""
        return [
            UploadExportToS3(
                formats=[Format.orc],  # type: ignore[attr-defined]
                object_types=self.object_types,
                s3_export_path=self.s3_export_path,
            )
        ]

    def output(self) -> List[luigi.Target]:
        """Returns an instance of :class:`AthenaDatabaseTarget`."""
        from .athena import TABLES

        return [AthenaDatabaseTarget(self.athena_db_name, set(TABLES))]

    def run(self) -> None:
        """Creates tables from the ORC dataset."""
        from .athena import create_tables

        create_tables(
            self.athena_db_name,
            self.s3_export_path,
            output_location=self.s3_athena_output_location,
            replace=True,
        )


class RunExportAll(luigi.WrapperTask):
    """Runs both the S3 and Athena export.

    Example invocation::

        luigi --local-scheduler --module swh.export.luigi RunExportAll \
                --ExportGraph-config=graph.staging.yml \
                --ExportGraph-processes=12 \
                --UploadExportToS3-local-export-path=/tmp/export_2022-11-08_staging/ \
                --formats=edges \
                --s3-export-path=s3://softwareheritage/graph/swh_2022-11-08 \
                --athena-db-name=swh_20221108 \
                --object-types=origin,origin_visit \
                --s3-athena-output-location=s3://softwareheritage/graph/tmp/athena
    """

    formats = luigi.EnumListParameter(
        enum=Format, batch_method=merge_lists, default=list(Format)
    )
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )
    s3_export_path = S3PathParameter()
    s3_athena_output_location = S3PathParameter()
    athena_db_name = luigi.Parameter()

    def requires(self) -> List[luigi.Task]:
        """Returns instances of :class:`CreateAthena` and :class:`UploadExportToS3`."""
        # CreateAthena depends on UploadExportToS3(formats=[edges]), so we need to
        # explicitly depend on UploadExportToS3(formats=self.formats) here, to also
        # export the formats requested by the user.
        return [
            CreateAthena(
                object_types=self.object_types,
                s3_export_path=self.s3_export_path,
                s3_athena_output_location=self.s3_athena_output_location,
                athena_db_name=self.athena_db_name,
            ),
            UploadExportToS3(
                formats=self.formats,
                object_types=self.object_types,
                s3_export_path=self.s3_export_path,
            ),
        ]
