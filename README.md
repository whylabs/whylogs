# WhyLogs Library
WhyLogs is the first of its kind statistical logging library. Whether you are running an experimentation or production ML/AI pipeline, understanding the properties of data that flows through an application is critical for stable model behavior and great customer experience. With WhyLogs, data science and ML teams can effortlessly instrument ML/AI pipelines and applications. Resulting log files are used for monitoring, alerting, analytics and error analysis. 

WhyLogs is an open source package that calculates approximate statistics for datasets of any size (from small to TB-size) in order to identify changes in the statistical properties of model's inputs or outputs. Approximate statistics allows the package to be deployed with minimal infrastructure requirements, and to work with an entire dataset as opposed to calculating actual statistics on a small sample of data which may miss outliers and other anomalies. These qualities make WhyLogs an excellent solution for instrumenting production ML/AI pipelines that operate on TB-scale data and with enterprise SLAs.  

[![PyPI version](https://badge.fury.io/py/whylogs.svg)](https://badge.fury.io/py/whylogs)
[![Python Version](https://img.shields.io/pypi/pyversions/whylogs)](https://pypi.org/project/whylogs/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/whylabs/whylogs-python/blob/mainline/LICENSE)

# Key Features

* **Data Insight** provides complex statistics across different stages of your ML/AI pipelines and applications.

* **Scalability** scales with your system, from local development mode to live production system in multi-node clusters. Suitable for batch and streaming architectures. 

* **Lightweight** using sketching algorithms and summarization statistics, WhyLogs produces small mergeable lightweight
  outputs in a variety of formats.

* **Unified data instrumentation** enable both data engineering pipelines and ML pipelines to share a common framework for tracking data quality and drifts. WhyLogs library supports multiple languages and many integrations. 
  
* **Observability** on top of supporting traditional monitoring approaches, WhyLogs data can support advanced ML-focused data quality and data drift detection, analytics and error analysis. 

# Examples

# Installation

### Using pip

[![PyPi Downloads](https://pepy.tech/badge/whylogs)](https://pepy.tech/project/whylogs)
[![PyPi Version](https://badge.fury.io/py/whylogs.svg)](https://pypi.org/project/whylogs/)

You can install using the pip package manager by running

    pip install whylogs
    
### From source

Download the source code by cloning the repository or by pressing ['Download ZIP'](https://github.com/whylabs/whylogs-python/archive/master.zip) on this page. 
Install by navigating to the proper directory and running

    python setup.py install

## Documentation

API documentation for `whylogs` can be found [whylogs.readthedocs.io](http://whylogs.readthedocs.io/).

### Demo CLI

Our demo CLI will generate a demo project flow by running:

     whylogs-demo

### Quick start CLI
WhyLogs can be configured programmatically or with our config YAML file. The quick start CLI can help you bootstrap the
configuration for your project. You can run the following command in the root of your Python project

     whylogs-quickstart
     
### Glossary/Concepts 
**Project:** A collection of related data sets that are used for multiple models or applications.

**Pipeline:** A series of one or multiple datasets to build a single model or application. A project might contain multiple pipelines.

**Dataset:** A collection of records. WhyLogs v0.0.2 supports structured datasets; meaning that the data can be represented as a table where each row is a different record, and each column is a feature of the record. 

**Feature:** In the context of WhyLogs v0.0.2 and structured data, a feature is a column in a dataset. A feature can be discrete (think of gender or eye color) or continuous (think of age or salary). 

**WhyLogs Output:** A profile summary file is returned by WhyLogs on a given dataset in JSON format. For convenience, files for this content are provided in flat table, histogram, and frequency format.

**Statistical Profile:** A collection of statistical properties of a given feature. Properties can be different for discrete and continuous features. 


### Integrations

### Dependencies
 
See [requirements.txt](https://github.com/whylabs/whylogs-python/blob/mainline/requirements.txt) for the core requirements.
For development environment, see [requirements-dev.txt](https://github.com/whylabs/whylogs-python/blob/mainline/requirements-dev.txt) for the development enviornment.