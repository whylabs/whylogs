.. _getting_started:

================
Getting Started
================

WhyLogs library comes with quickstart CLI to help you initialize the configuration. You can also use the API directly without \
going through the CLI.

Quick Start
===================

Install the Library
#####################
Install our library in a Python 3.6+ environment.

.. code-block:: bash

   pip install whylogs

Demo CLI
#######################
Our demo CLI will walk you through a .

.. code-block:: bash

   whylogs-quickstart demo

Configuration
===================
A WhyLogs config file contains the following parameters:

- **project** sets the name of the project.
- **pipeline** specifies the pipeline to be used.
- **verbose** sets output verbosity. Its default value is ``false``.
- **writers** specifies how and where output is stored, using path and filename templates that take the following variables:

    - project
    - pipeline
    - dataset_name
    - dataset_timestamp
    - session_timestamp
    
An example config file can be found `here <https://whylogs.readthedocs.io/en/latest/auto_examples/log_dataframe.html#sphx-glr-auto-examples-log-dataframe-py)>`_.

``whylogs.app.config.load_config()`` loads your config file. It attempts to load files at the following paths, in order:

1. The path set in the ``WHYLOGS_CONFIG`` environment variable
2. The current directory’s ``.whylogs.yaml`` file
3. ``~/.whylogs.yaml`` (in the home directory)
4. ``/opt/whylogs/.whylogs.yaml``


Using WhyLogs API
===================

Initialize a Logging Session
########################

An example script for creating a logging session can be found `here <https://whylogs.readthedocs.io/en/latest/auto_examples/log_dataframe.html#script>`__.

Create a Logger
########################

Loggers log statistical information about your data. They have the following parameters:

- **dataset_name** sets the name of the dataset, to be used in DatasetProfile metadata and generated filenames.
- **dataset_timestamp** sets a timestamp for the data.
- **session_timestamp** sets a timestamp for the creation of the session.
- **writers** provides a list of writers that will be used to create the DatasetProfile.
- **verbose** sets the verbosity of the output.

For more information, see the `documentation <https://whylogs.readthedocs.io/en/latest/autoapi/whylogs/app/logger/index.html>`_ for the logger class.

`This example code <https://whylogs.readthedocs.io/en/latest/auto_examples/configure_logger.html>`_ uses logger options to control the output location. 

Configure a Writer
########################

Writers write the statistics gathered by the logger into an output file. They use the following parameters to create output file paths:

- **output_path** sets the location output files will be stored. Use a directory path if your writer ``type = 'local'``, or a key prefix for ``type = 's3'``.
- **formats** lists all supported output formats.
- **path_template** optionally sets an output path using Python string templates.
- **filename_template** optionally sets output filenames using Python string templates.
- **dataset_timestamp** sets a timestamp for the data.
- **session_timestamp** sets a timestamp for the creation of the session.

For more information, see the `documentation <https://whylogs.readthedocs.io/en/latest/autoapi/whylogs/app/writers/index.html>`_ for the writer class.

Output WhyLogs data
########################

WhyLogs supports the following output formats:

- **Protobuf** is a lightweight binary format that maps one-to-one with the memory representation of a WhyLogs object. Use this format if you plan to apply advanced transformations to WhyLogs output.
- **JSON** displays the protobuf data in JSON format.
- **Flat** outputs multiple files with both CSV and JSON content to represent different views of the data, including histograms, upperbound, lowerbound, and frequent values.

