.. _swh-dataset-list:

Dataset
=======

We aim to provide regular exports of the Software Heritage graph in two
different formats:

- **Columnar data storage**: a set of relational tables stored in a columnar
  format such as `Apache ORC <https://orc.apache.org/>`_, which is particularly
  suited for scale-out analyses on data lakes and big data processing
  ecosystems such as the Hadoop environment.

- **Compressed graph**: a compact and highly-efficient representation of the
  graph dataset, suited for scale-up analysis on high-end machines with large
  amounts of memory. The graph is compressed in *Boldi-Vigna representation*,
  designed to be loaded by the `WebGraph framework
  <https://webgraph.di.unimi.it/>`_, specifically using our `swh-graph
  library <https://docs.softwareheritage.org/devel/swh-graph/index.html>`_.


Summary of dataset versions
---------------------------

**Full graph**:

.. list-table::
   :header-rows: 1

   * - Name
     - # Nodes
     - # Edges
     - Columnar
     - Compressed

   * - `2022-04-25`_
     - 25,340,003,875
     - 375,867,687,011
     - ✔
     - ✔

   * - `2021-03-23`_
     - 20,667,308,808
     - 232,748,148,441
     - ✔
     - ✔

   * - `2020-12-15`_
     - 19,330,739,526
     - 213,848,749,638
     - ✗
     - ✔

   * - `2020-05-20`_
     - 17,075,708,289
     - 203,351,589,619
     - ✗
     - ✔

   * - `2019-01-28`_
     - 11,683,687,950
     - 159,578,271,511
     - ✔
     - ✔


**Teaser datasets**:

.. list-table::
   :header-rows: 1

   * - Name
     - # Nodes
     - # Edges
     - Columnar
     - Compressed

   * - `2021-03-23-popular-3k-python`_
     - 45,691,499
     - 1,221,283,907
     - ✔
     - ✔

   * - `2020-12-15-gitlab-all`_
     - 1,083,011,764
     - 27,919,670,049
     - ✗
     - ✔

   * - `2020-12-15-gitlab-100k`_
     - 304,037,235
     - 9,516,984,175
     - ✗
     - ✔

   * - `2019-01-28-popular-4k`_
     - ?
     - ?
     - ✔
     - ✗

   * - `2019-01-28-popular-3k-python`_
     - 27,363,226
     - 346,413,337
     - ✔
     - ✔

Full graph datasets
-------------------

2022-04-25
~~~~~~~~~~

A full export of the graph dated from April 2022

- **Columnar tables (Apache ORC)**:

  - **Total size**: 11 TiB
  - **S3**: ``s3://softwareheritage/graph/2022-04-25/orc``

- **Compressed graph**:

  - **S3**: ``s3://softwareheritage/graph/2022-04-25/compressed``


2021-03-23
~~~~~~~~~~

A full export of the graph dated from March 2021.

- **Columnar tables (Apache ORC)**:

  - **Total size**: 8.4 TiB
  - **URL**: `/graph/2021-03-23/orc/
    <https://annex.softwareheritage.org/public/dataset/graph/2021-03-23/orc/>`_
  - **S3**: ``s3://softwareheritage/graph/2021-03-23/orc``

- **Compressed graph**:

  - **URL**: `/graph/2021-03-23/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2021-03-23/compressed/>`_


2020-12-15
~~~~~~~~~~

A full export of the graph dated from December 2020. Only available in
compressed representation.

- **Compressed graph**:

  - **URL**: `/graph/2020-12-15/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-12-15/compressed/>`_


2020-05-20
~~~~~~~~~~


A full export of the graph dated from May 2020. Only available in
compressed representation.
**(DEPRECATED: known issue with missing snapshot edges.)**

- **Compressed graph**:

  - **URL**: `/graph/2020-05-20/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-05-20/compressed/>`_


2019-01-28
~~~~~~~~~~

A full export of the graph dated from January 2019. The export was done in two
phases, one of them called "2018-09-25" and the other "2019-01-28". They both
refer to the same dataset, but the different formats have various
inconsistencies between them.
**(DEPRECATED: early export pipeline, various inconsistencies).**

- **Columnar tables (Apache Parquet)**:

  - **Total size**: 1.2 TiB
  - **URL**: `/graph/2019-01-28/parquet/
    <https://annex.softwareheritage.org/public/dataset/graph/2019-01-28/parquet/>`_
  - **S3**: ``s3://softwareheritage/graph/2018-09-25/parquet``

- **Compressed graph**:

  - **URL**: `/graph/2019-01-28/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2019-01-28/compressed/>`_


Teaser datasets
---------------

If the above datasets are too big, we also provide "teaser"
datasets that can get you started and have a smaller size fingerprint.

2021-03-23-popular-3k-python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``popular-3k-python`` teaser contains a subset of 2197 popular
repositories **tagged as being written in the Python language**, from GitHub,
Gitlab, PyPI and Debian. The selection criteria to pick the software origins
was the following:

- the 580 most popular GitHub projects written in Python (by number of stars),
- the 135 Gitlab projects written in Python that have 2 stars or more,
- the 827 most popular PyPI projects (by usage statistics, according to the
  `Top PyPI Packages <https://hugovk.github.io/top-pypi-packages/>`_ database),
- the 655 most popular Debian packages with the
  `debtag <https://debtags.debian.org/>`_ ``implemented-in::python`` (by
  "votes" according to the `Debian Popularity Contest
  <https://popcon.debian.org/>`_ database).

- **Columnar (Apache ORC)**:

  - **Total size**: 36 GiB
  - **S3**: ``s3://softwareheritage/graph/2021-03-23-popular-3k-python/orc/``

- **Compressed graph**:

  - **Total size**: 15 GiB
  - **S3**: ``s3://softwareheritage/graph/2021-03-23-popular-3k-python/compressed/``


2020-12-15-gitlab-all
~~~~~~~~~~~~~~~~~~~~~

A teaser dataset containing the entirety of Gitlab, exported in December 2020.
Available in compressed graph format.

- **Compressed graph**:

  - **URL**: `/graph/2020-12-15-gitlab-all/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-12-15-gitlab-all/compressed/>`_

2020-12-15-gitlab-100k
~~~~~~~~~~~~~~~~~~~~~~

A teaser dataset containing the 100k most popular Gitlab repositories,
exported in December 2020. Available in compressed graph format.

- **Compressed graph**:

  - **URL**: `/graph/2020-12-15-gitlab-100k/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-12-15-gitlab-100k/compressed/>`_


2019-01-28-popular-4k
~~~~~~~~~~~~~~~~~~~~~

This teaser dataset contains a subset of 4000 popular repositories from GitHub,
Gitlab, PyPI and Debian. The selection criteria to pick the software origins
was the following:

- The 1000 most popular GitHub projects (by number of stars)
- The 1000 most popular Gitlab projects (by number of stars)
- The 1000 most popular PyPI projects (by usage statistics, according to the
  `Top PyPI Packages <https://hugovk.github.io/top-pypi-packages/>`_ database),
- The 1000 most popular Debian packages (by "votes" according to the `Debian
  Popularity Contest <https://popcon.debian.org/>`_ database)

- **Columnar (Apache Parquet)**:

  - **Total size**: 27 GiB
  - **URL**: `/graph/2019-01-28-popular-4k/parquet/
    <https://annex.softwareheritage.org/public/dataset/graph/2019-01-28-popular-4k/parquet/>`_
  - **S3**: ``s3://softwareheritage/graph/2019-01-28-popular-4k/parquet/``

2019-01-28-popular-3k-python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``popular-3k-python`` teaser contains a subset of 3052 popular
repositories **tagged as being written in the Python language**, from GitHub,
Gitlab, PyPI and Debian. The selection criteria to pick the software origins
was the following, similar to ``popular-4k``:

- the 1000 most popular GitHub projects written in Python (by number of stars),
- the 131 Gitlab projects written in Python that have 2 stars or more,
- the 1000 most popular PyPI projects (by usage statistics, according to the
  `Top PyPI Packages <https://hugovk.github.io/top-pypi-packages/>`_ database),
- the 1000 most popular Debian packages with the
  `debtag <https://debtags.debian.org/>`_ ``implemented-in::python`` (by
  "votes" according to the `Debian Popularity Contest
  <https://popcon.debian.org/>`_ database).

- **Columnar (Apache Parquet)**:

  - **Total size**: 5.3 GiB
  - **URL**: `/graph/2019-01-28-popular-3k-python/parquet/
    <https://annex.softwareheritage.org/public/dataset/graph/2019-01-28-popular-3k-python/parquet/>`_
  - **S3**: ``s3://softwareheritage/graph/2019-01-28-popular-3k-python/parquet/``
