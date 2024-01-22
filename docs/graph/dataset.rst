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

See also :ref:`using-swh-data`.

.. admonition:: Terms of Use
   :name: remember-the-tos
   :class: important

   Usage of the datasets from the Software Heritage archive is covered by
   our `Ethical Charter`_ and the `Terms of use for bulk access`_.

.. _Ethical charter: https://www.softwareheritage.org/legal/users-ethical-charter/
.. _Terms of use for bulk access: https://www.softwareheritage.org/legal/bulk-access-terms-of-use/

.. raw:: html

   <script>
     window.addEventListener('load', function(_event) {
         try {
             if (localStorage.getItem("bulk-access-tos-agreed")) {
                 return;
             }
         } catch (_error) {
             // local storage not supported.
             // just proceed like the user never saw the button
         }
         let admonition = document.getElementById("remember-the-tos");
         let sections = document.querySelectorAll("#dataset > section");
         let button = document.createElement("button");
         button.id = "i-have-read-the-tos";
         button.innerHTML = "I will respect the Ethical Charter and Terms of Use";
         admonition.appendChild(button);
         sections.forEach(section => {
             section.style.display = "none";
         });
         document.getElementById("i-have-read-the-tos").onclick = function() {
             sections.forEach(section => {
                 section.style.display = "block";
             });
             document.getElementById("i-have-read-the-tos").style.display = "none";
             try {
                 localStorage.setItem("bulk-access-tos-agreed", true);
             } catch (_error) {
                 // unable to remember that the button has been clicked…
                 // too bad for the users, but not a big deal for us.
             }
         };
     });
   </script>

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

   * - `2023-09-06`_
     - 34,121,566,250
     - 517,399,308,984
     - ✔
     - ✔

   * - `2022-12-07`_
     - 27,397,574,122
     - 416,565,871,870
     - ✔
     - ✔

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

   * - `2023-09-06-popular-1k`_
     - 176,569,127
     - 11,322,432,687
     - ✔
     - ✔

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

Because of their size, some of the latest datasets are only available for
downside from Amazon S3.


.. _graph-dataset-2023-09-06:

2023-09-06
~~~~~~~~~~

A full export of the graph dated from September 2023

- **Columnar tables (Apache ORC)**:

  - **Total size**: 15 TiB
  - **S3**: ``s3://softwareheritage/graph/2023-09-06/orc``

- **Compressed graph**:

  - **Total size**: 8.8 TiB
  - **S3**: ``s3://softwareheritage/graph/2023-09-06/compressed``


.. _graph-dataset-2022-12-07:

2022-12-07
~~~~~~~~~~

A full export of the graph dated from December 2022

- **Columnar tables (Apache ORC)**:

  - **Total size**: 13 TiB
  - **S3**: ``s3://softwareheritage/graph/2022-12-07/orc``

- **Compressed graph**:

  - **Total size**: 7.1 TiB
  - **S3**: ``s3://softwareheritage/graph/2022-12-07/compressed``

- **"History and hosting" Compressed graph**:

  - This is a compressed graph of only the "history and hosting" layer (origins,
    snapshots, releases, revisions) and the root directory (or rarely content) of
    every revision/release; but most directories and contents are excluded
  - **Total size**: 1 TiB
  - **S3**: ``s3://softwareheritage/graph/2022-12-07-history-hosting/compressed``

- **Erratum**:

  - `author and committer timestamps were shifted back 1 or 2 hours, based on the Europe/Paris timezone <https://gitlab.softwareheritage.org/swh/devel/swh-graph/-/issues/4788>`_


.. _graph-dataset-2022-04-25:

2022-04-25
~~~~~~~~~~

A full export of the graph dated from April 2022

- **Columnar tables (Apache ORC)**:

  - **Total size**: 11 TiB
  - **S3**: ``s3://softwareheritage/graph/2022-04-25/orc``

- **Compressed graph**:

  - **Total size**: 6.5 TiB
  - **S3**: ``s3://softwareheritage/graph/2022-04-25/compressed``


.. _graph-dataset-2021-03-23:

2021-03-23
~~~~~~~~~~

A full export of the graph dated from March 2021.

- **Columnar tables (Apache ORC)**:

  - **Total size**: 8.4 TiB
  - **URL**: `/graph/2021-03-23/orc/
    <https://annex.softwareheritage.org/public/dataset/graph/2021-03-23/orc/>`_
  - **S3**: ``s3://softwareheritage/graph/2021-03-23/orc``

- **Compressed graph**:

  - **S3**: ``s3://softwareheritage/graph/2021-03-23/compressed``


.. _graph-dataset-2020-12-15:

2020-12-15
~~~~~~~~~~

A full export of the graph dated from December 2020. Only available in
compressed representation.

- **Compressed graph**:

  - **URL**: `/graph/2020-12-15/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-12-15/compressed/>`_


.. _graph-dataset-2020-05-20:

2020-05-20
~~~~~~~~~~


A full export of the graph dated from May 2020. Only available in
compressed representation.
**(DEPRECATED: known issue with missing snapshot edges.)**

- **Compressed graph**:

  - **URL**: `/graph/2020-05-20/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-05-20/compressed/>`_


.. _graph-dataset-2019-01-28:

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


.. _graph-dataset-2023-09-06-popular-1k:

2023-09-06-popular-1k
~~~~~~~~~~~~~~~~~~~~~

The ``popular-1k`` teaser contains a subset of 1120 popular repositories **tagged
as being written in one of the 10 most popular languages** (Javascript, Python, Java,
Typescript, C#, C++, PHP, Shell, C, Ruby), from GitHub,
Gitlab.com, Packagist, PyPI and Debian. The selection criteria to pick the software origins
for each language was the following:

- the 50 most popular Gitlab.com projects written in that languagethat have 2 stars or more,
- for Python, the 50 most popular PyPI projects (by usage statistics, according to the
  `Top PyPI Packages <https://hugovk.github.io/top-pypi-packages/>`_ database),
- for PHP, the 50 most popular Packagist projects (by usage statistics, according to
  `Packagist's API <https://packagist.org/apidoc#list-popular-packages>`_),
- the 50 most popular Debian packages with the relevant ``implemented-in::``
  `debtag <https://debtags.debian.org/>`_ (by "installs" according to the
  `Debian Popularity Contest <https://popcon.debian.org/>`_ database).
- most popular GitHub projects written in Python (by number of stars), until the total
  number of origins for that language reaches 200
- removing origins not archived by |swh| by 2023-09-06

- **Columnar (Apache ORC)**:

  - **Total size**: 280 GiB
  - **S3**: ``s3://softwareheritage/graph/2023-09-06-popular-1k/orc/``

- **Compressed graph**:

  - **Total size**: 42 GiB
  - **S3**: ``s3://softwareheritage/graph/2023-09-06-popular-1k/compressed/``


.. _graph-dataset-2021-03-23-popular-3k-python:

2021-03-23-popular-3k-python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``popular-3k-python`` teaser contains a subset of 2197 popular
repositories **tagged as being written in the Python language**, from GitHub,
Gitlab.com, PyPI and Debian. The selection criteria to pick the software origins
was the following:

- the 580 most popular GitHub projects written in Python (by number of stars),
- the 135 Gitlab.com projects written in Python that have 2 stars or more,
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


.. _graph-dataset-2020-12-15-gitlab-all:

2020-12-15-gitlab-all
~~~~~~~~~~~~~~~~~~~~~

A teaser dataset containing the entirety of Gitlab.com, exported in December 2020.
Available in compressed graph format.

- **Compressed graph**:

  - **URL**: `/graph/2020-12-15-gitlab-all/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-12-15-gitlab-all/compressed/>`_


.. _graph-dataset-2020-12-15-gitlab-100k:

2020-12-15-gitlab-100k
~~~~~~~~~~~~~~~~~~~~~~

A teaser dataset containing the 100k most popular Gitlab.com repositories,
exported in December 2020. Available in compressed graph format.

- **Compressed graph**:

  - **URL**: `/graph/2020-12-15-gitlab-100k/compressed/
    <https://annex.softwareheritage.org/public/dataset/graph/2020-12-15-gitlab-100k/compressed/>`_


.. _graph-dataset-2019-01-28-popular-4k:

2019-01-28-popular-4k
~~~~~~~~~~~~~~~~~~~~~

This teaser dataset contains a subset of 4000 popular repositories from GitHub,
Gitlab.com, PyPI and Debian. The selection criteria to pick the software origins
was the following:

- The 1000 most popular GitHub projects (by number of stars)
- The 1000 most popular Gitlab.com projects (by number of stars)
- The 1000 most popular PyPI projects (by usage statistics, according to the
  `Top PyPI Packages <https://hugovk.github.io/top-pypi-packages/>`_ database),
- The 1000 most popular Debian packages (by "votes" according to the `Debian
  Popularity Contest <https://popcon.debian.org/>`_ database)

- **Columnar (Apache Parquet)**:

  - **Total size**: 27 GiB
  - **URL**: `/graph/2019-01-28-popular-4k/parquet/
    <https://annex.softwareheritage.org/public/dataset/graph/2019-01-28-popular-4k/parquet/>`_
  - **S3**: ``s3://softwareheritage/graph/2019-01-28-popular-4k/parquet/``

.. _graph-dataset-2019-01-28-popular-3k-python:

2019-01-28-popular-3k-python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``popular-3k-python`` teaser contains a subset of 3052 popular
repositories **tagged as being written in the Python language**, from GitHub,
Gitlab.com, PyPI and Debian. The selection criteria to pick the software origins
was the following, similar to ``popular-4k``:

- the 1000 most popular GitHub projects written in Python (by number of stars),
- the 131 Gitlab.com projects written in Python that have 2 stars or more,
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
