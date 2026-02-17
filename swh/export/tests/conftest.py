# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.journal.writer import get_journal_writer


@pytest.fixture
def journal_client_config(
    kafka_server: str, kafka_prefix: str, kafka_consumer_group: str
):
    return dict(
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
    )


@pytest.fixture
def journal_writer(kafka_server: str, kafka_prefix: str):
    return get_journal_writer(
        cls="kafka",
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        anonymize=False,
    )


@pytest.fixture(autouse=True)
def thread_pool_executor_for_workers(mocker):
    """Use threads instead of processes to execute the graph export workers
    in parallel while running tests so code coverage can be computed."""
    from concurrent.futures import ThreadPoolExecutor

    from swh.export import journalprocessor

    mocker.patch.object(journalprocessor, "ProcessPoolExecutor", ThreadPoolExecutor)
