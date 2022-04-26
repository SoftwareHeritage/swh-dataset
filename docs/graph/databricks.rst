Setup on Azure Databricks
=========================

.. highlight:: python

This tutorial will explain you how you can load the dataset in an Azure Spark
cluster, and interface with it using a Python notebook in Azure Databricks.


Preliminaries
-------------

Make sure you have:

- familiarized yourself with the `Azure Databricks Getting Started Guide
  <https://docs.azuredatabricks.net/getting-started/index.html>`_

- uploaded the dataset in the Parquet format on Azure (the most efficient place
  to upload it is an `Azure Data Lake Storage Gen2
  <https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction>`_
  container).

- created a Spark cluster in the Databricks interface and attached a Python
  notebook to it.

- set the OAuth credentials in the Notebook so that your parquet files are
  accessible from the notebook, as described `here
  <https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake-gen2.html#dataframe-or-dataset-api>`_.

To ensure that you have completed all the preliminary steps, run the following
command in your Notebook::

    dataset_path = 'abfss://YOUR_CONTAINER@YOUR_ACCOUNT.dfs.core.windows.net/PARQUET_FILES_PATH'
    dbutils.fs.ls(dataset_path)

You should see an output like this::

    [FileInfo(path='abfss://.../swh/content/', name='content/', size=0),
     FileInfo(path='abfss://.../swh/directory/', name='directory/', size=0),
     ...]

Loading the tables
------------------

We need to load the Parquet tables as temporary views in Spark::

    def register_table(table):
      abfss_path = dataset_path + '/' + table
      df = spark.read.parquet(abfss_path)
      print("Register the DataFrame as a SQL temporary view: {} (path: {})"
            .format(table, abfss_path))
      df.createOrReplaceTempView(table_name)

    tables = [
      'content',
      'directory',
      'directory_entry',
      'origin',
      'origin_visit',
      'origin_visit_status',
      'release',
      'revision',
      'revision_history',
      'skipped_content',
      'snapshot',
      'snapshot_branch',
    ]

    for table in tables:
      register_table(table)

Running queries
---------------

You can now execute PySpark methods on the tables::

    df = spark.sql("select id from origin limit 10")
    display(df)

.. highlight:: sql

It is also possible to use the ``%sql`` magic command in the Notebook to
directly preview SQL results::

    %sql
    select id from origin limit 10
