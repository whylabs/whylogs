# WhyLogs Library
[![PyPI version](https://badge.fury.io/py/whylogs.svg)](https://badge.fury.io/py/whylogs)
[![Python Version](https://img.shields.io/pypi/pyversions/whylogs)](https://pypi.org/project/whylogs/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/whylabs/whylogs-python/blob/mainline/LICENSE)

Understanding the properties of data as it moves through applications is essential to keeping your ML/AI pipeline stable and improving your user experience, whether your pipeline is built for production or experimentation.. WhyLogs is an open source statistical logging library that allows data science and ML teams to effortlessly profile ML/AI pipelines and applications, producing log files that can be used for monitoring, alerts, analytics, and error analysis. 

WhyLogs calculates approximate statistics for datasets of any size up to TB-scale, making it easy for users to identify changes in the statistical properties of a model's inputs or outputs. Using approximate statistics allows the package to run on minimal infrastructure and monitor an entire dataset, rather than miss outliers and other anomalies by calculating more precise statistics on only a small data sample. These qualities make WhyLogs an excellent solution for profiling production ML/AI pipelines that operate on TB-scale data and with enterprise SLAs.  

# Key Features

* **Data Insight:** WhyLogs provides complex statistics across different stages of your ML/AI pipelines and applications.

* **Scalability:** WhyLogs scales with your system, from local development mode to live production systems in multi-node clusters, and works well with batch and streaming architectures. 

* **Lightweight:** WhyLogs produces small mergeable lightweight outputs in a variety of formats, using sketching algorithms and summarizing statistics.

* **Unified data instrumentation:** To enable data engineering pipelines and ML pipelines to share a common framework for tracking data quality and drifts, the WhyLogs library supports multiple languages and integrations. 
  
* **Observability:** In addition to supporting traditional monitoring approaches, WhyLogs data can support advanced ML-focused analytics, error analysis, and data quality and data drift detection. 

# Examples
- [Logging a Dataframe](https://whylogs.readthedocs.io/en/latest/auto_examples/log_dataframe.html)
- [Logger Options](https://whylogs.readthedocs.io/en/latest/auto_examples/configure_logger.html#sphx-glr-auto-examples-configure-logger-py)

# Installation

### Using pip

[![PyPi Downloads](https://pepy.tech/badge/whylogs)](https://pepy.tech/project/whylogs)
[![PyPi Version](https://badge.fury.io/py/whylogs.svg)](https://pypi.org/project/whylogs/)

Install WhyLogs using the pip package manager by running

    pip install whylogs
    
### From source

Download the source code by cloning the repository or by pressing ['Download ZIP'](https://github.com/whylabs/whylogs-python/archive/master.zip) on this page. 
Install by navigating to the desired directory and running

    python setup.py install

## Documentation

API documentation for `whylogs` can be found at [whylogs.readthedocs.io](http://whylogs.readthedocs.io/).

### Demo CLI

Our demo CLI generates a demo project flow by running

     whylogs-demo

### Quick start CLI
WhyLogs can be configured programmatically or by using our config YAML file. The quick start CLI can help you bootstrap the
configuration for your project. To use the quick start CLI, run the following command in the root of your Python project.

     whylogs-quickstart
     
### Glossary/Concepts 
**Project:** A collection of related data sets used for multiple models or applications.

**Pipeline:** One or more datasets used to build a single model or application. A project may contain multiple pipelines.

**Dataset:** A collection of records. WhyLogs v0.0.2 supports structured datasets, which represent data as a table where each row is a different record and each column is a feature of the record. 

**Feature:** In the context of WhyLogs v0.0.2 and structured data, a feature is a column in a dataset. A feature can be discrete (like gender or eye color) or continuous (like age or salary). 

**WhyLogs Output:** WhyLogs returns profile summary files for a dataset in JSON format. For convenience, these files are provided in flat table, histogram, and frequency formats.

**Statistical Profile:** A collection of statistical properties of a feature. Properties can be different for discrete and continuous features.

### Integrations
The WhyLogs library is integrated with the following:
- NumPy and Pandas
- [Java and Apache Spark](https://github.com/whylabs/whylogs-java)
- AWS S3 (for output storage)
- Jupyter Notebooks

### Dependencies
 
For the core requirements, see [requirements.txt](https://github.com/whylabs/whylogs-python/blob/mainline/requirements.txt).

For the development environment, see [requirements-dev.txt](https://github.com/whylabs/whylogs-python/blob/mainline/requirements-dev.txt).

# Development/contributing
For more information on contributing to WhyLogs, see [`DEVELOPMENT.md`](DEVELOPMENT.md).
