[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/whylabs/whylogs-python/blob/mainline/LICENSE)
[![PyPI version](https://badge.fury.io/py/whylogs.svg)](https://badge.fury.io/py/whylogs)
[![Coverage Status](https://coveralls.io/repos/github/whylabs/whylogs-python/badge.svg?branch=mainline&service=github)](https://coveralls.io/github/whylabs/whylogs-python?branch=mainline)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4490/badge)](https://bestpractices.coreinfrastructure.org/projects/4490)
[![PyPi Downloads](https://pepy.tech/badge/whylogs)](https://pepy.tech/project/whylogs)
![CI](https://github.com/whylabs/whylogs-python/workflows/whylogs%20CI/badge.svg)
[![Maintainability](https://api.codeclimate.com/v1/badges/442f6ca3dca1e583a488/maintainability)](https://codeclimate.com/github/whylabs/whylogs-python/maintainability)



whylogs is an open source standard for data and ML logging, monitoring, and troubleshooting

Whylogs logging agent is the easiest way to enable logging, testing, and monitoring in an ML/AI application. The lightweight agent profiles data in real time, collecting thousands of metrics from structured data, unstructured data, and ML model predictions with zero configuration. 

Whylogs can be installed in any Python, Java or Spark environment; it can be deployed as a container and run as a sidecar; or invoked through various ML tools (see integrations). 

Whylogs is designed by data scientists, ML engineers and distributed systems engineers to log data in the most cost-effective, scalable and accurate manner. No sampling. No post-processing. No manual configurations.

whylogs is released under the Apache 2.0 open source license. It supports many languages and is easy to extend. This repo contains the whylogs CLI, language SDKs, and individual libraries are in their own repos.


This is a Python implementation of whylogs. The Java implementation can be found [here](https://github.com/whylabs/whylogs-java).

If you have any questions, comments, or just want to hang out with us, please join [our Slack channel](http://join.slack.whylabs.ai/).


_____________
Latest release: 0.4.5

_____________


- [Getting started](#getting-started)
- [Features](#features)
- [Data Types](#data-types)
- [Examples](#examples)
- [Integrations](#integrations)
- [Community](#community)
- [Roadmap](#roadmap)
- [Contribute](#contribute)



## Getting started<a name="getting-started" />


### Using pip

Install whylogs using the pip package manager by running

```
pip install whylogs
```

### From the source 

- Download the source code by cloning the repository or by pressing [Download ZIP](https://github.com/whylabs/whylogs-python/archive/master.zip) on this page. 

- Install [poetry](https://python-poetry.org/docs/#installation) for package management

- Then running:

```
make install
make
```

### Documentation 

The [documentation](https://docs.whylabs.ai/docs/) of this package is generated automatically. 

### Features

- Accurate data profiling: whylogs calculates statistics from 100% of the data, never requiring sampling, ensuring an accurate representation of data distributions
- Lightweight runtime: whylogs utilizes approximate statistical methods to achieve minimal memory footprint that scales with the number of features in the data
- Any architecture: whylogs scales with your system, from local development mode to live production systems in multi-node clusters, and works well with batch and streaming architectures
-Configuration-free: whylogs infers the schema of the data, requiring zero manual configuration to get started
-Tiny storage footprint: whylogs turns data batches and streams into statistical fingerprints, 10-100MB uncompressed
-Unlimited metrics: whylogs collects all possible statistical metrics about structured or unstructured data


## Data Types<a name="data-types" />
Whylogs supports both structured and unstructured data, specifically: 

| Data type  | Features | Notebook Example |
| --- | --- | ---|
|Structured data | Distribution, cardinality, schema, counts, missing values | [Getting started with structure data](https://github.com/whylabs/whylogs-examples/blob/mainline/python/GettingStarted.ipynb) | 
| Images | exif metadata, derived pixels features,  bounding boxes | [Getting started with images](https://github.com/whylabs/whylogs-examples/blob/mainline/python/Logging_Images.ipynb) |
|Video  | In development  | [Github Issue #214](https://github.com/whylabs/whylogs/issues/214) | 
| Tensors | derived 1d features (more in developement) |  [Github Issue #216](https://github.com/whylabs/whylogs/issues/216) |
| Embeddings | derived 1d features (more in developement) |  |
| Text | top k values, counts, cardinality (more in developement) | [Github Issue #213](https://github.com/whylabs/whylogs/issues/213)|
| Audio | In developement | [Github Issue #212](https://github.com/whylabs/whylogs/issues/212) | 


## Integrations

| Integration | Features | Notebook | Blog Post |
| --- | --- | ---  | --- | 
| Spark | Log and monitor any Spark dataframe | | | 
| Pandas | Log and monitor any pandas dataframe | [Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/python/logging_example.ipynb) | [whylogs: Embrace Data Logging](https://whylabs.ai/blog/posts/whylogs-embrace-data-logging) |
| Kafka | Log and monitor Kafka topics with whylogs| [Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/python/Kafka.ipynb) | [Integrating whylogs into your Kafka ML Pipeline](https://whylabs.ai/blog/posts/integrating-whylogs-into-your-kafka-ml-pipeline) |
| MLflow | Enhance MLflow metrics with whylogs:  | [Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/python/MLFlow%20Integration%20Example.ipynb) | [Streamlining data monitoring with whylogs and MLflow](https://whylabs.ai/blog/posts/on-model-lifecycle-and-monitoring) |
| Github actions | Unit test data with whylogs and github actions| [Notebook Example](https://github.com/whylabs/whylogs-examples/tree/mainline/github-actions) | |
| RAPIDS |  Use whylogs in RAPIDS environment | [Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/python/RAPIDS%20GPU%20Integration%20Example.ipynb)| [Monitoring High-Performance Machine Learning Models with RAPIDS and whylogs](https://whylabs.ai/blog/posts/monitoring-high-performance-machine-learning-models-with-rapids-and-whylogs) |
| Java | Run whylogs in Java environment| [Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/java/demo1/src/main/java/com/whylogs/examples/WhyLogsDemo.java) | |
| Scala | Run whylogs in Scala environment|  [Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/scala/src/main/scala/WhyLogsDemo.scala) | |
| Docker | Run whylogs as in Docker |  | |
| AWS S3 |  Store whylogs profiles in S3 | [S3 example](https://github.com/whylabs/whylogs-examples/blob/mainline/python/S3%20example.ipynb)

 
## Roadmap

whylogs is maintained by [WhyLabs](https://whylabs.ai).

## Community

If you have any questions, comments, or just want to hang out with us, please join [our Slack channel](http://join.slack.whylabs.ai/).


## Contribute

For more information on contributing to whylogs, see [DEVELOPMENT.md](https://github.com/whylabs/whylogs/blob/mainline/DEVELOPMENT.md).


 



