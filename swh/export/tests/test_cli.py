# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest
import yaml

from swh.export.cli import export_cli_group
from swh.model.tests import swh_model_data

from .utils import (
    assert_contents_exported_to_orc,
    assert_directories_exported_to_orc,
    assert_origin_visit_statuses_exported_to_orc,
    assert_origin_visits_exported_to_orc,
    assert_origins_exported_to_orc,
    assert_releases_exported_to_orc,
    assert_revisions_exported_to_orc,
    assert_skipped_contents_exported_to_orc,
    assert_snapshots_exported_to_orc,
    disable_gc,
    orc_load,
)


@disable_gc
@pytest.mark.parametrize(
    "objects,object_type,assert_objects_exported_to_orc",
    [
        (swh_model_data.CONTENTS, "content", assert_contents_exported_to_orc),
        (
            swh_model_data.SKIPPED_CONTENTS,
            "skipped_content",
            assert_skipped_contents_exported_to_orc,
        ),
        (swh_model_data.DIRECTORIES, "directory", assert_directories_exported_to_orc),
        (swh_model_data.RELEASES, "release", assert_releases_exported_to_orc),
        (swh_model_data.REVISIONS, "revision", assert_revisions_exported_to_orc),
        (swh_model_data.SNAPSHOTS, "snapshot", assert_snapshots_exported_to_orc),
        (swh_model_data.ORIGINS, "origin", assert_origins_exported_to_orc),
        (
            # FIXME: Some visits for an origin with same date but with different ids
            #        are not exported, only the first one is
            swh_model_data.ORIGIN_VISITS[:-2],
            "origin_visit",
            assert_origin_visits_exported_to_orc,
        ),
        (
            swh_model_data.ORIGIN_VISIT_STATUSES,
            "origin_visit_status",
            assert_origin_visit_statuses_exported_to_orc,
        ),
    ],
    ids=[
        "content",
        "skipped_content",
        "directory",
        "release",
        "revision",
        "snapshot",
        "origin",
        "origin_visit",
        "origin_visit_status",
    ],
)
def test_cli_graph_export_to_orc(
    journal_client_config,
    journal_writer,
    cli_runner,
    tmp_path,
    objects,
    object_type,
    assert_objects_exported_to_orc,
):

    journal_writer.write_additions(object_type, objects)

    config_path = tmp_path / "export_config.yml"
    export_path = tmp_path / "graph_export"
    export_path.mkdir()
    with open(config_path, "w") as config:
        yaml.dump(
            {
                "journal": journal_client_config,
                "masking_db": None,
            },
            config,
        )
    result = cli_runner.invoke(
        export_cli_group,
        args=[
            "-C",
            str(config_path),
            "graph",
            "export",
            "--export-id",
            f"test-{object_type}",
            "--export-name",
            object_type,
            "--types",
            object_type,
            str(export_path),
        ],
    )
    assert result.exit_code == 0, result.output

    orcs = orc_load(export_path / "orc")

    assert object_type in orcs

    assert_params = [objects, orcs[object_type]]

    if object_type == "directory":
        assert_params.append(orcs["directory_entry"])
    elif object_type == "revision":
        assert_params.append(orcs["revision_history"])
    elif object_type == "snapshot":
        assert_params.append(orcs["snapshot_branch"])

    assert_objects_exported_to_orc(*assert_params)
