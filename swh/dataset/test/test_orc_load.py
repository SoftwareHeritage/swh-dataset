import re

import attrs
import pytest

from swh.dataset.orc_loader import load_ORC
from swh.dataset.relational import MAIN_TABLES
from swh.model.tests.swh_model_data import TEST_OBJECTS

from .test_orc import orc_export


@pytest.mark.parametrize("obj_type", MAIN_TABLES.keys())
def test_load_origins(obj_type):
    config = {"orc": {"group_tables": True}}
    objects = TEST_OBJECTS[obj_type]
    if obj_type == "content":
        objects = [attrs.evolve(obj, data=None, ctime=None) for obj in objects]
    with orc_export({obj_type: TEST_OBJECTS[obj_type]}, config=config) as export_dir:
        for orc_file in (export_dir / obj_type).iterdir():
            m = re.match(r"(?P<type>[a-z_]+)-(?P<uuid>[0-9a-f-]+).orc", orc_file.name)
            if m:
                assert m.group("type") == obj_type
                uuid = m.group("uuid")
                for obj in load_ORC(export_dir, obj_type, uuid):
                    assert obj in objects
