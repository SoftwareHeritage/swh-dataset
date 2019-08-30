Dataset
=======

We provide the full graph dataset along with two "teaser" datasets that can be
used for trying out smaller-scale experiments before using the full graph.

All the main URLs are relative to our dataset prefix:
`https://annex.softwareheritage.org/public/dataset/ <https://annex.softwareheritage.org/public/dataset/>`__.

The Software Heritage Graph Dataset contains a table representation of the full
Software Heritage Graph.  It is available in the following formats:

- **PostgreSQL (compressed)**:

  - **URL**: `/graph/latest/sql/
    <https://annex.softwareheritage.org/public/dataset/graph/latest/sql/>`_
  - **Total size**: 1.2 TiB

- **Apache Parquet**:

  - **URL**: `/graph/latest/parquet/
    <https://annex.softwareheritage.org/public/dataset/graph/latest/parquet/>`_
  - **Total size**: 1.2 TiB

Teaser datasets
---------------

If the above dataset is too big, we also provide the following "teaser"
datasets that can get you started and have a smaller size fingerprint.

popular-4k
~~~~~~~~~~

The ``popular-4k`` teaser contains a subset of 4000 popular
repositories from GitHub, Gitlab, PyPI and Debian. The selection criteria to
pick the software origins was the following:

- The 1000 most popular GitHub projects (by number of stars)
- The 1000 most popular Gitlab projects (by number of stars)
- The 1000 most popular PyPI projects (by usage statistics, according to the
  `Top PyPI Packages <https://hugovk.github.io/top-pypi-packages/>`_ database),
- The 1000 most popular Debian packages (by "votes" according to the `Debian
  Popularity Contest <https://popcon.debian.org/>`_ database)

This teaser is available in the following formats:

- **PostgreSQL (compressed)**:

  - **URL**: `/graph/latest/popular-4k/sql/
    <https://annex.softwareheritage.org/public/dataset/graph/latest/popular-4k/sql/>`_
  - **Total size**: TODO

- **Apache Parquet**:

  - **URL**: `/graph/latest/popular-4k/parquet/
    <https://annex.softwareheritage.org/public/dataset/graph/latest/popular-4k/parquet/>`_
  - **Total size**: TODO

popular-3k-python
~~~~~~~~~~~~~~~~~

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

- **PostgreSQL (compressed)**:

  - **URL**: `/graph/latest/popular-3k-python/sql/
    <https://annex.softwareheritage.org/public/dataset/graph/latest/popular-3k-python/sql/>`_
  - **Total size**: TODO

- **Apache Parquet**:

  - **URL**: `/graph/latest/popular-3k-python/sql/
    <https://annex.softwareheritage.org/public/dataset/graph/latest/popular-3k-python/parquet/>`_
  - **Total size**: TODO
