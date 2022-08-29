.. _swh-graph-athena:

Setup on Amazon Athena
======================

The Software Heritage Graph Dataset is available as a public dataset in `Amazon
Athena <https://aws.amazon.com/athena/>`_. Athena uses `presto
<https://prestodb.github.io/>`_, a distributed SQL query engine, to
automatically scale queries on large datasets.

The pricing of Athena depends on the amount of data scanned by each query,
generally at a cost of $5 per TiB of data scanned. Full pricing details are
available `here <https://aws.amazon.com/athena/pricing/>`_.

Note that because the Software Heritage Graph Dataset is available as a public
dataset, you **do not have to pay for the storage, only for the queries**
(except for the data you store on S3 yourself, like query results).


Loading the tables
------------------

.. highlight:: bash

AWS account
~~~~~~~~~~~

In order to use Amazon Athena, you will first need to `create an AWS account
and setup billing
<https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/>`_.

You will also need to create an **output S3 bucket**: this is the place where
Athena will store your query results, so that you can retrieve them and analyze
them afterwards.  To do that, go on the `S3 console
<https://s3.console.aws.amazon.com/s3/home>`_ and create a new bucket.


Setup
~~~~~

Athena needs to be made aware of the location and the schema of the Parquet
files available as a public dataset. Unfortunately, since Athena does not
support queries that contain multiple commands, it is not as simple as pasting
an installation script in the console. Instead, you can use the ``swh dataset
athena`` command on your local machine, which will query Athena to create
the tables automatically with the appropriate schema.

First, install the ``swh.dataset`` Python module from PyPI::

    pip install swh.dataset

Once the dependencies are installed, run::

    aws configure

This will ask for an AWS Access Key ID and an AWS Secret Access Key in
order to give the Boto3 library access to your AWS account. These keys can be
generated at `this address
<https://console.aws.amazon.com/iam/home#/security_credentials>`_.

It will also ask for the region in which you want to run the queries. We
recommend to use ``us-east-1``, since that's where the public dataset is
located.

Creating the tables
~~~~~~~~~~~~~~~~~~~

The ``swh dataset athena create`` command can be used to create the tables on
your Athena instance. For example, to create the tables of the 2021-03-23
graph::

    swh dataset athena create \
        --database-name swh_graph_2021_03_23
        --location-prefix s3://softwareheritage/graph/2021-03-23
        --output-location s3://YOUR_OUTPUT_BUCKET/

To check that the tables have been successfully created in your account, you
can open your `Amazon Athena console
<https://console.aws.amazon.com/athena/home>`_. You should be able to select
the database corresponding to your dataset, and see the tables:

.. image:: _images/athena_tables.png


Running queries
---------------

From the console, once you have selected the database of your dataset, you can
run SQL queries directly from the Query Editor.

Try for instance this query that computes the most frequent file names in the
archive:

.. code-block:: sql

    SELECT from_utf8(name, '?') AS name, COUNT(DISTINCT target) AS cnt
    FROM directory_entry
    GROUP BY name
    ORDER BY cnt DESC
    LIMIT 10;

Other examples are available in the preprint of our article: `The Software
Heritage Graph Dataset: Public software development under one roof.
<https://upsilon.cc/~zack/research/publications/msr-2019-swh.pdf>`_

It is also possible to query Athena directly from the command line, using the
``swh dataset athena query`` command::

    echo "select message from revision limit 10;" |
    swh dataset athena query \
        --database-name swh_graph_2021_03_23
        --output-location s3://YOUR_OUTPUT_BUCKET/
