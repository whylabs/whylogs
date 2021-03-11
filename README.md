# whylogs Library
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/whylabs/whylogs-python/blob/mainline/LICENSE)
[![PyPI version](https://badge.fury.io/py/whylogs.svg)](https://badge.fury.io/py/whylogs)
[![Coverage Status](https://coveralls.io/repos/github/whylabs/whylogs-python/badge.svg?branch=mainline&service=github)](https://coveralls.io/github/whylabs/whylogs-python?branch=mainline)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4490/badge)](https://bestpractices.coreinfrastructure.org/projects/4490)
[![PyPi Downloads](https://pepy.tech/badge/whylogs)](https://pepy.tech/project/whylogs)

![CI](https://github.com/whylabs/whylogs-python/workflows/whylogs%20CI/badge.svg)
[![Maintainability](https://api.codeclimate.com/v1/badges/442f6ca3dca1e583a488/maintainability)](https://codeclimate.com/github/whylabs/whylogs-python/maintainability)

This is a Python implementation of whylogs. The Java implementation can be found [here](https://github.com/whylabs/whylogs-java).

Understanding the properties of data as it moves through applications is essential to keeping your ML/AI pipeline stable
 and improving your user experience, whether your pipeline is built for production or experimentation. whylogs is an 
 open source statistical logging library that allows data science and ML teams to effortlessly profile ML/AI pipelines and applications, producing log files that can be used for monitoring, alerts, analytics, and error analysis. 

whylogs calculates approximate statistics for datasets of any size up to TB-scale, making it easy for users to identify
 changes in the statistical properties of a model's inputs or outputs. Using approximate statistics allows the package 
 to run on minimal infrastructure and monitor an entire dataset, rather than miss outliers and other anomalies by only 
 using a sample of the data to calculate statistics. These qualities make whylogs an excellent solution for profiling 
 production ML/AI pipelines that operate on TB-scale data and with enterprise SLAs.  
 
For questions and discussions, hop on our [slack channel](http://join.slack.whylabs.ai/)!

# Key Features

* **Data Insight:** whylogs provides complex statistics across different stages of your ML/AI pipelines and applications.

* **Scalability:** whylogs scales with your system, from local development mode to live production systems in multi-node 
clusters, and works well with batch and streaming architectures. 

* **Lightweight:** whylogs produces small mergeable lightweight outputs in a variety of formats, using sketching 
algorithms and summarizing statistics.

* **Unified data instrumentation:** To enable data engineering pipelines and ML pipelines to share a common framework 
for tracking data quality and drifts, the whylogs library supports multiple languages and integrations. 
  
* **Observability:** In addition to supporting traditional monitoring approaches, whylogs data can support advanced 
ML-focused analytics, error analysis, and data quality and data drift detection. 

## Statistical Profile
whylogs collects approximate statistics and sketches of data on a column-basis into a statistical profile. 
These metrics include:

* **Simple counters**: boolean, null values, data types.
* **Summary statistics**: sum, min, max, variance.
* **Unique value counter** or **cardinality**: tracks an approximate unique value of your feature using HyperLogLog algorithm.
* **Histograms** for numerical features. whylogs binary output can be queried to with dynamic binning based on the 
shape of your data. 
* **Top frequent items** (default is 128). Note that this configuration affects the memory footprint, especially for text features.

# Examples
For a full set of our examples, please check out [whylogs-examples](https://github.com/whylabs/whylogs-examples).

Note that to use the run with matplotlib vizualiation, you'll have to install whylogs with `viz` dependencies:
```
pip install "whylogs[viz]"
```

Check out our example notebooks with Binder: [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/whylabs/whylogs-examples/HEAD)
- [Getting Started notebook](https://github.com/whylabs/whylogs-examples/blob/mainline/python/GettingStarted.ipynb)
- [Logging Example notebook](https://github.com/whylabs/whylogs-examples/blob/mainline/python/logging_example.ipynb)
- [Logging Images](https://github.com/whylabs/whylogs-examples/blob/mainline/python/Logging_Images.ipynb)
- [MLflow Integration](https://github.com/whylabs/whylogs-examples/blob/mainline/python/MLFlow%20Integration%20Example.ipynb)

# Installation

### Using pip

[![PyPi Downloads](https://pepy.tech/badge/whylogs)](https://pepy.tech/project/whylogs)
[![PyPi Version](https://badge.fury.io/py/whylogs.svg)](https://pypi.org/project/whylogs/)

Install whylogs using the pip package manager by running

    pip install whylogs
    
### From source

Download the source code by cloning the repository or by pressing ['Download ZIP'](https://github.com/whylabs/whylogs-python/archive/master.zip) on this page. 
Install by navigating to the desired directory and running

    python setup.py install

## Documentation

API documentation for `whylogs` can be found at [whylogs.readthedocs.io](http://whylogs.readthedocs.io/).

### Demo CLI

Our demo CLI generates a demo project flow by running

     whylogs-demo init

### Quick start CLI
whylogs can be configured programmatically or by using our config YAML file. The quick start CLI can help you bootstrap the
configuration for your project. To use the quick start CLI, run the following command in the root of your Python project.

     whylogs init
     
### Glossary/Concepts 
**Project:** A collection of related data sets used for multiple models or applications.

**Pipeline:** One or more datasets used to build a single model or application. A project may contain multiple pipelines.

**Dataset:** A collection of records. whylogs v0.0.2 supports structured datasets, which represent data as a table 
where each row is a different record and each column is a feature of the record. 

**Feature:** In the context of whylogs v0.0.2 and structured data, a feature is a column in a dataset. A feature can 
be discrete (like gender or eye color) or continuous (like age or salary). 

**whylogs Output:** whylogs returns profile summary files for a dataset in JSON format. For convenience, these files 
are provided in flat table, histogram, and frequency formats.

**Statistical Profile:** A collection of statistical properties of a feature. Properties can be different for discrete 
and continuous features.

### Integrations
The whylogs library is integrated with the following:
- NumPy and Pandas
- [Java and Apache Spark](https://github.com/whylabs/whylogs-java)
- AWS S3 (for output storage)
- Jupyter Notebooks

### Dependencies
 
For the core requirements, see [requirements.txt](https://github.com/whylabs/whylogs-python/blob/mainline/requirements.txt).

For the development environment, see [requirements-dev.txt](https://github.com/whylabs/whylogs-python/blob/mainline/requirements-dev.txt).

# Development/contributing
For more information on contributing to whylogs, see [`DEVELOPMENT.md`](DEVELOPMENT.md).

# Who maintains whylogs?
whylogs is maintained by [WhyLabs](https://whylabs.ai).

If you have any questions, comments, or just want to hang out with us, please join [our Slack channel](http://join.slack.whylabs.ai/).

If you want to see whylogs in action in enterprise settings with complex visualizations, check out the [WhyLabs Platform Sandbox](http://try.whylabsapp.com/).
You'll need a GitHub/Google/LinkedIn account to login to view the sandbox (it's a 1-click experience!).
