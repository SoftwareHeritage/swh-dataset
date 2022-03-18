from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Type, cast

from pyorc import Reader, StructRepr, TypeKind
from pyorc.converters import ORCConverter

from swh.dataset.exporters.orc import SWHTimestampConverter
from swh.model import model as swhmodel
from swh.model.hashutil import hash_to_bytes

SWH_OBJECT_TYPES = {
    cls.object_type: cls  # type: ignore
    for cls in (
        swhmodel.Origin,
        swhmodel.OriginVisit,
        swhmodel.OriginVisitStatus,
        swhmodel.Snapshot,
        swhmodel.SnapshotBranch,
        swhmodel.Release,
        swhmodel.Revision,
        swhmodel.Directory,
        swhmodel.DirectoryEntry,
        swhmodel.Content,
        swhmodel.SkippedContent,
        swhmodel.MetadataAuthority,
        swhmodel.MetadataFetcher,
        swhmodel.RawExtrinsicMetadata,
        swhmodel.ExtID,
    )
}


# basic utility functions


def hash_to_bytes_or_none(hash: Optional[str]) -> Optional[bytes]:
    return hash_to_bytes(hash) if hash is not None else None


def orc_to_swh_date(d, prefix):
    timestamp = d.pop(f"{prefix}")
    offset_bytes = d.pop(f"{prefix}_raw_offset_bytes")
    if prefix == "committer_date":
        d.pop("committer_offset")
    else:
        d.pop(f"{prefix}_offset")
    sec, usec = timestamp if timestamp else (None, None)

    if sec is None:
        d[prefix] = None
    else:
        d[prefix] = {
            "timestamp": {"seconds": sec, "microseconds": usec},
            "offset_bytes": offset_bytes,
        }


def orc_to_datetime(sec, usec):
    return datetime.fromtimestamp(sec, timezone.utc) + timedelta(microseconds=usec)


# obj_type converters


def cvrt_release(d, relations=None):
    d = d.copy()
    d["author"] = (
        swhmodel.Person.from_fullname(d["author"]).to_dict() if d["author"] else None
    )
    orc_to_swh_date(d, "date")
    if "synthetic" not in d:
        d["synthetic"] = False
    d["id"] = hash_to_bytes(d["id"])
    if d["target"]:
        d["target"] = hash_to_bytes_or_none(d["target"])
    if "target_type" not in d:
        d["target_type"] = swhmodel.TargetType.REVISION
    return d


def cvrt_revision(d, relations=None):
    parents = relations["revision_history"]
    headers = relations["revision_extra_headers"]
    d = d.copy()
    d["author"] = (
        swhmodel.Person.from_fullname(d["author"]).to_dict() if d["author"] else None
    )
    d["committer"] = (
        swhmodel.Person.from_fullname(d["committer"]).to_dict()
        if d["committer"]
        else None
    )
    orc_to_swh_date(d, "date")
    orc_to_swh_date(d, "committer_date")
    d["id"] = hash_to_bytes(d["id"])
    d["directory"] = hash_to_bytes_or_none(d["directory"])
    d["parents"] = [
        hash_to_bytes_or_none(p["parent_id"])
        for p in sorted(parents[d["id"]], key=lambda x: x["parent_rank"])
    ]
    if "synthetic" not in d:
        d["synthetic"] = False
    d["extra_headers"] = (
        [(h["key"], h["value"]) for h in headers[d["id"]]] if headers[d["id"]] else ()
    )
    return d


def cvrt_origin_visit_status(d, relations=None):
    d = d.copy()
    d["snapshot"] = hash_to_bytes_or_none(d["snapshot"])
    d["date"] = orc_to_datetime(*d["date"])
    return d


def cvrt_origin_visit(d, relations=None):
    d = d.copy()
    d["date"] = orc_to_datetime(*d["date"])
    return d


def cvrt_snapshot(d, relations=None):
    d = d.copy()
    d["id"] = hash_to_bytes(d["id"])
    branches = [
        (
            br["name"],
            {
                "target": hash_to_bytes_or_none(br["target"]),
                "target_type": br["target_type"],
            },
        )
        for br in relations["snapshot_branch"][d["id"]]
    ]
    d["branches"] = dict(branches)
    return d


def cvrt_snapshot_branch(d):
    return (
        d["name"],
        {"target": hash_to_bytes_or_none(d["target"]), "target_type": d["target_type"]},
    )


def cvrt_directory(d, relations=None):
    d = d.copy()
    d_id = hash_to_bytes(d.pop("id"))
    entries = [
        {
            "name": entry["name"],
            "type": entry["type"],
            "target": hash_to_bytes_or_none(entry["target"]),
            "perms": entry["perms"],
        }
        for entry in relations["directory_entry"][d_id]
    ]
    d["entries"] = entries
    return d


def cvrt_content(d, relations=None):
    d = d.copy()
    d.update(
        {k: hash_to_bytes(d[k]) for k in ("sha1", "sha256", "sha1_git", "blake2s256")}
    )
    return d


LOADERS = {
    "origin_visit": cvrt_origin_visit,
    "origin_visit_status": cvrt_origin_visit_status,
    "release": cvrt_release,
    "revision": cvrt_revision,
    "snapshot": cvrt_snapshot,
    "directory": cvrt_directory,
    "content": cvrt_content,
    "skipped_content": cvrt_content,
}

RELATION_TYPES = {
    "snapshot": ("snapshot_branch",),
    "revision": ("revision_history", "revision_extra_headers",),
    "directory": ("directory_entry",),
}


# ORC loader functions
def _load_related_ORC(orc_file: Path) -> Tuple[str, Dict[str, List]]:
    with orc_file.open("rb") as fileobj:
        reader = Reader(fileobj, struct_repr=StructRepr.DICT)
        relation_obj_type = reader.user_metadata.get("swh_object_type", b"").decode()
        if relation_obj_type == "snapshot_branch":
            id_col = "snapshot_id"
        elif relation_obj_type == "directory_entry":
            id_col = "directory_id"
        else:
            id_col = "id"
        # XXX this loads the whole relation file content in memory...
        relations = defaultdict(list)
        convert = LOADERS.get(relation_obj_type, lambda x: x)
        for relation in (cast(Dict, x) for x in reader):
            relations[hash_to_bytes(relation[id_col])].append(convert(relation))
        return relation_obj_type, relations


def load_ORC(orc_dir: Path, obj_type: str, uuid: str) -> Iterator[swhmodel.ModelType]:
    """Load a set of ORC files and generates swh.model objects

    From the ORC dataset directory orc_dir, load the ORC file named

      {orc_dir}/{obj_type}/{obj_type}-{uuid}.orc

    and generates swh data model objects (of type obj_type) that have been
    serialized in the ORC dataset. This may need to load related ORC dataset
    files (for example loading a revision dataset file also requires the
    related revision_history dataset).

    Related ORC files are expected to be found using the same file name schema.
    For example, the revision_history dataset file is expected to be found at:

      {orc_dir}/revision_history/revision_history-{uuid}.orc

    Thus using the same uuid as the main dataset file.
    """
    all_relations = {}

    orc_file = orc_dir / obj_type / f"{obj_type}-{uuid}.orc"
    with orc_file.open("rb") as fileobj:
        reader = Reader(
            fileobj,
            struct_repr=StructRepr.DICT,
            converters={
                TypeKind.TIMESTAMP: cast(Type[ORCConverter], SWHTimestampConverter)
            },
        )
        obj_type = reader.user_metadata.get("swh_object_type", b"").decode()
        if obj_type in RELATION_TYPES:
            for relation_type in RELATION_TYPES[obj_type]:
                orc_relation_file = (
                    orc_dir / relation_type / f"{relation_type}-{uuid}.orc"
                )
                assert orc_relation_file.is_file()
                relation_obj_type, relations = _load_related_ORC(orc_relation_file)
                assert relation_obj_type == relation_type
                all_relations[relation_type] = relations

        convert = LOADERS.get(obj_type, lambda x, y: x)
        swhcls = cast(Type[swhmodel.ModelType], SWH_OBJECT_TYPES[obj_type])
        for objdict in reader:
            obj = cast(
                swhmodel.ModelType, swhcls.from_dict(convert(objdict, all_relations))
            )
            yield obj
