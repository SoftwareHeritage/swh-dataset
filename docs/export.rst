.. _swh-graph-export:

===================
Exporting a dataset
===================

This repository aims to contain various pipelines to generate datasets of
Software Heritage data, so that they can be used internally or by external
researchers.

Graph dataset
=============

Exporting the full dataset
--------------------------

Right now, the only supported export pipeline is the *Graph Dataset*, a set of
relational tables representing the Software Heritage Graph, as documented in
:ref:`swh-graph-dataset`. It can be run using the ``swh dataset graph export``
command.

This dataset can be exported in two different formats: ``orc`` and ``edges``.
To export a graph, you need to provide a comma-separated list of formats to
export with the ``--formats`` option. You also need an export ID, a unique
identifier used by the Kafka server to store the current progress of the
export.

**Note**: exporting as the ``edges`` format is discouraged, as it is redundant
and can easily be generated directly from the ORC format.

Here is an example command to start a graph dataset export::

    swh dataset -C graph_export_config.yml graph export \
        --formats orc \
        --export-id 2022-04-25 \
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


Uploading on S3 & on the annex
------------------------------

The dataset should then be made available publicly by uploading it on S3 and on
the public annex.

For S3::

    aws s3 cp --recursive /srv/softwareheritage/hdd/graph/2022-04-25/orc s3://softwareheritage/graph/2022-04-25/orc

For the annex::

    scp -r 2022-04-25/orc saam.internal.softwareheritage.org:/srv/softwareheritage/annex/public/dataset/graph/2022-04-25/
    ssh saam.internal.softwareheritage.org
    cd /srv/softwareheritage/annex/public/dataset/graph
    git annex add 2022-04-25
    git annex sync --content


Documenting the new dataset
---------------------------

In the ``swh-dataset`` repository, edit the the file ``docs/graph/dataset.rst``
to document the availability of the new dataset. You should usually mention:

- the name of the dataset version (e.g., 2022-04-25)
- the number of nodes
- the number of edges
- the available formats (notably whether the graph is also available in its
  compressed representation).
- the total on-disk size of the dataset
- the buckets/URIs to obtain the graph from S3 and from the annex
