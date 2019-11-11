Setup on a PostgreSQL instance
==============================

This tutorial will guide you through the steps required to setup the Software
Heritage Graph Dataset in a PostgreSQL database.

.. highlight:: bash

PostgreSQL local setup
----------------------

You need to have access to a running PostgreSQL instance to load the dataset.
This section contains information on how to setup PostgreSQL for the first
time.

*If you already have a PostgreSQL server running on your machine, you can skip
to the next section.*

- For **Ubuntu** and **Debian**::

    sudo apt install postgresql

- For **Archlinux**::

    sudo pacman -S --needed postgresql
    sudo -u postgres initdb -D '/var/lib/postgres/data'
    sudo systemctl enable --now postgresql

Once PostgreSQL is running, you also need an user that will be able to create
databases and run queries. The easiest way to achieve that is simply to create
an account that has the same name as your username and that can create
databases::

    sudo -u postgres createuser --createdb $USER


Retrieving the dataset
----------------------

You need to download the dataset in SQL format. Use the following command on
your machine, after making sure that it has enough available space for the
dataset you chose:

.. tabs::

  .. group-tab:: full

    ::

      mkdir swhgd && cd swhgd
      wget -c -q --show-progress -A gz,sql -nd -r -np -nH https://annex.softwareheritage.org/public/dataset/graph/latest/sql/

  .. group-tab:: teaser: popular-4k

    ::

      mkdir popular-4k && cd popular-4k
      wget -c -q --show-progress -A gz,sql -nd -r -np -nH https://annex.softwareheritage.org/public/dataset/graph/latest/popular-4k/sql/

  .. group-tab:: teaser: popular-3k-python

    ::

      mkdir popular-3k-python && cd popular-3k-python
      wget -c -q --show-progress -A gz,sql -nd -r -np -nH https://annex.softwareheritage.org/public/dataset/graph/latest/popular-3k-python/sql/

Loading the dataset
-------------------

Once you have retrieved the dataset of your choice, create a database that will
contain it, and load the database:

.. tabs::

  .. group-tab:: full

    ::

      createdb swhgd
      psql swhgd < load.sql

  .. group-tab:: teaser: popular-4k

    ::

      createdb swhgd-popular-4k
      psql swhgd-popular-4k < load.sql

  .. group-tab:: teaser: popular-3k-python

    ::

      createdb swhgd-popular-3k-python
      psql swhgd-popular-3k-python < load.sql


You can now run SQL queries on your database. Run ``psql <database_name>`` to
start an interactive PostgreSQL console.
