===================
Exporting a dataset
===================

This repository aims to contain various pipelines to generate datasets of
Software Heritage data, so that they can be used internally or by external
researchers.

Graph dataset
=============

Right now, the only supported export pipeline is the *Graph Dataset*, a set of
relational tables representing the Software Heritage Graph, as documented in
:ref:`swh-graph-dataset`. It can be run using the ``swh dataset graph export``
command.

This dataset can be exported in two different formats: ``orc`` and ``edges``.
To export a graph, you need to provide a comma-separated list of formats to
export with the ``--formats`` option. You also need an export ID, a unique
identifier used by the Kafka server to store the current progress of the
export.

Here is an example command to start a graph dataset export::

    swh dataset -C graph_export_config.yml graph export \
        --formats orc \
        --export-id seirl-2022-04-25 \
        -p 64 \
        /srv/softwareheritage/hdd/graph/2022-04-25

This command usually takes more than a week for a full export, it is
therefore advised to run it in a service or a tmux session.

The configuration file should contain the configuration for the swh-journal
clients, as well as various configuration options for the exporters. Here is an
example configuration file::

    journal:
        brokers:
            - kafka1.internal.softwareheritage.org:9094
            - kafka2.internal.softwareheritage.org:9094
            - kafka3.internal.softwareheritage.org:9094
            - kafka4.internal.softwareheritage.org:9094
        security.protocol: SASL_SSL
        sasl.mechanisms: SCRAM-SHA-512
        max.poll.interval.ms: 1000000

    remove_pull_requests: true


The following configuration options can be used for the export:

- ``remove_pull_requests``: remove all edges from origin to snapshot matching
  ``refs/*`` but not matching ``refs/heads/*`` or ``refs/tags/*``. This removes
  all the pull requests that are present in Software Heritage (archived with
  ``git clone --mirror``).
