.. _swh-graph-dataset:

Software Heritage Graph Dataset
===============================

This is the Software Heritage graph dataset: a fully-deduplicated Merkle
DAG representation of the Software Heritage archive. The dataset links
together file content identifiers, source code directories, Version
Control System (VCS) commits tracking evolution over time, up to the
full states of VCS repositories as observed by Software Heritage during
periodic crawls. The dataset’s contents come from major development
forges (including `GitHub <https://github.com/>`__ and
`GitLab <https://gitlab.com>`__), FOSS distributions (e.g.,
`Debian <debian.org>`__), and language-specific package managers (e.g.,
`PyPI <https://pypi.org/>`__). Crawling information is also included,
providing timestamps about when and where all archived source code
artifacts have been observed in the wild.

The Software Heritage graph dataset is available in multiple formats,
including relational Apache ORC files for local use, as well as a public
instance on Amazon Athena interactive query service for ready-to-use powerful
analytical processing.

By accessing the dataset, you agree with the Software Heritage `Ethical
Charter for using the archive
data <https://www.softwareheritage.org/legal/users-ethical-charter/>`__,
and the `terms of use for bulk
access <https://www.softwareheritage.org/legal/bulk-access-terms-of-use/>`__.


If you use this dataset for research purposes, please cite the following paper:

*
    | Antoine Pietri, Diomidis Spinellis, Stefano Zacchiroli.
    | *The Software Heritage Graph Dataset: Public software development under one roof.*
    | In proceedings of `MSR 2019 <http://2019.msrconf.org/>`_: The 16th International Conference on Mining Software Repositories, May 2019, Montreal, Canada. Co-located with `ICSE 2019 <https://2019.icse-conferences.org/>`_.
    | `preprint <https://upsilon.cc/~zack/research/publications/msr-2019-swh.pdf>`_, `bibtex <https://upsilon.cc/~zack/research/publications/msr-2019-swh.bib>`_

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :titlesonly:

   dataset
   schema
   athena
   databricks


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
