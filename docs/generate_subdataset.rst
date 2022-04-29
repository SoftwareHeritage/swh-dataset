.. _swh-graph-export-subdataset:

======================
Exporting a subdataset
======================

.. highlight:: bash

Because the entire graph is often too big to be practical for many research use
cases, notably for prototyping, it is generally useful to publish "subdatasets"
which only contain a subset of the entire graph.
An example of a very useful subdataset is the graph containing only the top
1000 most popular GitHub repositories (sorted by number of stars).

This page details the various steps required to export a graph subdataset using
swh-graph and Amazon Athena.


Step 1. Obtain the list of origins
----------------------------------

You first need to obtain a list of origins that you want to include in the
subdataset. Depending on the type of subdataset you want to create, this can be
done in various ways, either manual or automated. The following is an example
of how to get the list of the 1000 most popular GitHub repositories in the
Python language, sorted by number of stars::

    for i in $( seq 1 10 ); do \
        curl -G https://api.github.com/search/repositories \
            -d "page=$i" \
            -d "s=stars" -d "order=desc" -d "q=language:python" -d 'per_page=100' | \
            jq --raw-output '.items[].html_url'; \
        sleep 6; \
    done > origins.txt


Step 2. Build the list of SWHIDs
--------------------------------

To generate a subdataset from an existing dataset, you need to generate the
list of all the SWHIDs to include in the subdataset. The best way to achieve
that is to use the compressed graph to perform a full visit of the compressed
graph starting from the origin nodes, and to return the list of all the SWHIDs
that are reachable from these origins.

Unfortunately, there is currently no endpoint in the HTTP API to start a
traversal from multiple nodes. The current best way to achieve this is
therefore to visit the graph starting from each origin, one by one, and then to
merge all the resulting lists of SWHIDs into a single sorted list of unique
SWHIDs.

If you use the internal graph API, you might need to convert the origin URLs in
the Extended SWHID format (``swh:ori:1:<sha1(url)>``) to query the API.


Step 3. Generate the subdataset on Athena
-----------------------------------------

Once you have obtained a text file containing all the SWHIDs to be included in
the new dataset, it is possible to use AWS Athena to JOIN this list of SWHIDs
with the tables of an existing dataset, and write the output as a new ORC
dataset.

First, make sure that your base dataset containing the entire graph is
available as a database on AWS Athena, which can be set up by
following the steps described in :ref:`swh-graph-athena`.

The subdataset can then be generated with the ``aws dataset athena
gensubdataset`` command::

    swh dataset athena gensubdataset \
        --swhids swhids.csv \
        --database swh_20210323
        --subdataset-database swh_20210323_popular3kpython \
        --subdataset-location s3://softwareheritage/graph/2021-03-23-popular-3k-python/


Step 4. Upload and document the newly generated subdataset
----------------------------------------------------------

After having executed the previous step, there should now be a new dataset
located at the S3 path given as the parameter to ``--subdataset-location``.
You can upload, publish and document this new subdataset by following the
procedure described in :ref:`swh-graph-export`.
