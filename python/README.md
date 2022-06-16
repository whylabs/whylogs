<img src="https://static.scarf.sh/a.png?x-pxid=bc3c57b0-9a65-49fe-b8ea-f711c4d35b82" /><p align="center">
<img src="https://i.imgur.com/nv33goV.png" width="35%"/>
</br>

<h1 align="center">The open standard for data logging

 </h1>
  <h3 align="center">
   <a href="https://docs.whylabs.ai/docs/"><b>Documentation</b></a> &bull;
   <a href="https://bit.ly/whylogsslack"><b>Slack Community</b></a> &bull;
   <a href="https://github.com/whylabs/whylogs#python-quickstart"><b>Python Quickstart</b></a>
 </h3>

<p align="center">
<a href="https://github.com/whylabs/whylogs-python/blob/mainline/LICENSE" target="_blank">
    <img src="http://img.shields.io/:license-Apache%202-blue.svg" alt="License">
</a>
<a href="https://badge.fury.io/py/whylogs" target="_blank">
    <img src="https://badge.fury.io/py/whylogs.svg" alt="PyPi Version">
</a>
<a href="https://github.com/python/black" target="_blank">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Code style: black">
</a>
<a href="https://pepy.tech/project/whylogs" target="_blank">
    <img src="https://pepy.tech/badge/whylogs" alt="PyPi Downloads">
</a>
<a href="bit.ly/whylogs" target="_blank">
    <img src="https://github.com/whylabs/whylogs-python/workflows/whylogs%20CI/badge.svg" alt="CI">
</a>
<a href="https://codeclimate.com/github/whylabs/whylogs-python/maintainability" target="_blank">
    <img src="https://api.codeclimate.com/v1/badges/442f6ca3dca1e583a488/maintainability" alt="Maintainability">
</a>
</p>

## What is whylogs

whylogs is an open source library for logging any kind of data. With whylogs, users are able to generate summaries of their datasets (called _whylogs profiles_) which they can use to:

1. Track changes in their dataset
2. Create _data constraints_ to know whether their data looks the way it should
3. Quickly visualize key summary statistics about their datasets

These three functionalities enable a variety of use cases for data scientists, machine learning engineers, and data engineers:

- Detect data drift in model input features
- Detect training-serving skew, concept drift, and model performance degradation
- Validate data quality in model inputs or in a data pipeline
- Perform exploratory data analysis of massive datasets
- Track data distributions & data quality for ML experiments
- Enable data auditing and governance across the organization
- Standardize data documentation practices across the organization
- And more

whylogs can be run in Python or [Apache Spark](https://docs.whylabs.ai/docs/spark-integration) (both PySpark and Scala) environments on a variety of [data types](#data-types). We [integrate](#integrations) with lots of other tools including Pandas, [AWS Sagemaker](https://aws.amazon.com/blogs/startups/preventing-amazon-sagemaker-model-degradation-with-whylabs/), [MLflow](https://docs.whylabs.ai/docs/mlflow-integration), [Flask](https://whylabs.ai/blog/posts/deploy-and-monitor-your-ml-application-with-flask-and-whylabs), [Ray](https://docs.whylabs.ai/docs/ray-integration), [RAPIDS](https://whylabs.ai/blog/posts/monitoring-high-performance-machine-learning-models-with-rapids-and-whylogs), [Apache Kafka](https://docs.whylabs.ai/docs/kafka-integration), and more.

If you have any questions, comments, or just want to hang out with us, please join [our Slack Community](https://bit.ly/rsqrd-slack). In addition to joining the Slack Community, you can also help this project by giving us a ⭐ in the upper right corner of this page.

## Python Quickstart<a name="python-quickstart" />

Installing whylogs using the pip package manager is as easy as running `pip install whylogs` in your terminal.

From here, you can quickly log a dataset:

```python
import whylogs as why
import pandas as pd

#dataframe
df = pd.read_csv("path/to/file.csv")
results = why.log(df)
```

And voila, you now have a whylogs profile. To learn more about about a whylogs profile is and what you can do with it, read on.

## Table of Contents

- [whylogs Profiles](#whylogs-profiles)
- [Data Constraints](#data-constraints)
- [Profile Visualization](#profile-visualization)
- [Integrations](#integrations)
- [Supported Data Types](#data-types)
- [Examples](#examples)
- [Usage Statistics](#usage-statistics)
- [Community](#community)
- [Contribute](#contribute)

## whylogs Profiles<a name="whylogs-profiles" />

### What are profiles

whylogs profiles are the core of the whylogs library. They capture key statistical properties of data, such as the distribution (far beyond simple mean, median, and standard deviation measures), the number of missing values, and a wide range of configurable custom metrics. By capturing these summary statistics, we are able to accurately represent the data and enable all of the use cases described in the introduction.

whylogs profiles have three properties that make them ideal for data logging: they are **efficient**, **customizable**, and **mergeable**.

<br />

<img align="left" src="https://user-images.githubusercontent.com/7946482/171064257-26bf727e-3480-4ec3-9c9d-5d8a79567bca.png">

**Efficient**: whylogs profiles efficiently describe the dataset that they represent. This high fidelity representation of datasets is what enables whylogs profiles to be effective snapshots of the data. They are better at capturing the characteristics of a dataset than a sample would be—as discussed in our [Data Logging: Sampling versus Profiling](https://whylabs.ai/blog/posts/data-logging-sampling-versus-profiling) blog post—and are very compact.

<br />

<img align="left" src="https://user-images.githubusercontent.com/7946482/171064575-72ee0f76-7365-4fd1-9cab-4debb673baa8.png">

**Customizable**: The statistics that whylogs profiles collect are easily configured and customizable. This is useful because different data types and use cases require different metrics, and whylogs users need to be able to easily define custom trackers for those metrics. It’s the customizability of whylogs that enables our text, image, and other complex data trackers.

<br />

<img align="left" src="https://user-images.githubusercontent.com/7946482/171064525-2d314534-6cdb-4c07-9d9f-5c74d5c03029.png">

**Mergeable**: One of the most powerful features of whylogs profiles is their mergeability. Mergeability means that whylogs profiles can be combined together to form new profiles which represent the aggregate of their constituent profiles. This enables logging for distributed and streaming systems, and allows users to view aggregated data across any time granularity.

<br />

### How do you generate profiles

Once whylogs is installed, it's easy to generate profiles in both Python and Java environments.

To generate a profile from a Pandas dataframe in Python, simply run:

```python
import whylogs as why
import pandas as pd

#dataframe
df = pd.read_csv("path/to/file.csv")
results = why.log(df)
```

<!---
For images, replace `df` with `image="path/to/image.png"`. Similarly, you can profile Python dicts by replacing the dataframe within the `log()` function with a Python `dict` object.
--->

### What can you do with profiles

Once you’ve generated whylogs profiles, a few things can be done with them:

In your local Python environment, you can set data constraints or visualize your profiles. Setting data constraints on your profiles allows you to get notified when your data don’t match your expectations, allowing you to do data unit testing and some baseline data monitoring. With the Profile Visualizer, you can visually explore your data, allowing you to understand it and ensure that your ML models are ready for production.

In addition, you can send whylogs profiles to the SaaS ML monitoring and AI observability platform [WhyLabs](https://whylabs.ai). With WhyLabs, you can automatically set up monitoring for your machine learning models, getting notified on both data quality and data change issues (such as data drift). If you’re interested in trying out WhyLabs, check out the always free [Starter edition](https://whylabs.ai/free), which allows you to experience the entire platform’s capabilities with no credit card required.

## Data Constraints<a name="data-constraints" />

Constraints are a powerful feature built on top of whylogs profiles that enable you to quickly and easily validate that your data looks the way that it should. There are numerous types of constraints that you can set on your data (that numerical data will always fall within a certain range, that text data will always be in a JSON format, etc) and, if your dataset fails to satisfy a constraint, you can fail your unit tests or your CI/CD pipeline.

A simple example of setting and testing a constraint is:

```python
import whylogs as why
from whylogs.core.constraints import Constraints, ConstraintsBuilder, MetricsSelector, MetricConstraint

profile_view = why.log(df).view()
builder = ConstraintsBuilder(profile_view)

builder.add_constraint(MetricConstraint(
    name="col_name >= 0",
    condition=lambda x: x.min >= 0,
    metric_selector=MetricsSelector(metric_name='distribution', column_name='col_name')
))
constraints: Constraints = builder.build()
constraints.report()
```

To learn more about constraints, check out: the [Constraints Example](https://bit.ly/whylogsconstraintsexample).

## Profile Visualization<a name="profile-visualization" />

In addition to being able to automatically get notified about potential issues in data, it’s also useful to be able to inspect your data manually. With the profile visualizer, you can generate interactive reports about your profiles (either a single profile or comparing profiles against each other) directly in your Jupyter notebook environment. This enables exploratory data analysis, data drift detection, and data observability.

To access the profile visualizer, install the `[viz]` module of whylogs by running `pip install whylogs[viz]` in your terminal. One type of profile visualization that we can create is a drift report; here's a simple example of how to analyze the drift between two profiles:

```python
import whylogs as why

from whylogs.viz import NotebookProfileVisualizer

result = why.log(pandas=df_target)
prof_view = result.view()

result_ref = why.log(pandas=df_reference)
prof_view_ref = result_ref.view()

visualization = NotebookProfileVisualizer()
visualization.set_profiles(target_profile_view=prof_view, reference_profile_view=prof_view_ref)

visualization.summary_drift_report()
```

![image](https://user-images.githubusercontent.com/7946482/169669536-a25cce95-acde-4637-b7b9-c2a685f0bc3f.png)

To learn more about visualizing your profiles, check out: the [Visualizer Example](https://bit.ly/whylogsvisualizerexample)

## Data Types<a name="data-types" />

whylogs supports both structured and unstructured data, specifically:

| Data type        | Features | Notebook Example                                                                                                                                                |
| ---------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tabular Data     | ✅       | [Getting started with structured data](https://github.com/whylabs/whylogs/blob/mainline/python/examples/basic/Getting_Started.ipynb)                            |
| Image Data       | ✅       | [Getting started with images](https://github.com/whylabs/whylogs/blob/maintenance/0.7.x/examples/logging_images.ipynb)                                          |
| Text Data        | ✅       | [String Features](https://github.com/whylabs/whylogs/blob/maintenance/0.7.x/examples/String_Features.ipynb)                                                     |
| Embeddings       | 🛠        |                                                                                                                                                                 |
| Other Data Types | ✋       | Do you have a request for a data type that you don’t see listed here? Raise an issue or join our Slack community and make a request! We’re always happy to help |

## Integrations

![current integration](https://user-images.githubusercontent.com/7946482/171062942-01c420f2-7768-4b7c-88b5-e3f291e1b7d8.png)
| Integration | Features | Resources |
| --- | --- | --- |
| Spark | Run whylogs in Apache Spark environment| <ul><li>[Code Example](https://github.com/whylabs/whylogs/blob/mainline/python/examples/integrations/Pyspark_Profiling.ipynb)</li></ul> |
| Pandas | Log and monitor any pandas dataframe | <ul><li>[Notebook Example](https://github.com/whylabs/whylogs/blob/mainline/python/examples/basic/Getting_Started.ipynb)</li><li>[whylogs: Embrace Data Logging](https://whylabs.ai/blog/posts/whylogs-embrace-data-logging)</li></ul> |
| MLflow | Enhance MLflow metrics with whylogs: | <ul><li>[Notebook Example](https://github.com/whylabs/whylogs/blob/mainline/python/examples/integrations/Mlflow_Logging.ipynb)</li><li>[Streamlining data monitoring with whylogs and MLflow](https://whylabs.ai/blog/posts/on-model-lifecycle-and-monitoring)</li></ul> |
| Java | Run whylogs in Java environment| <ul><li>[Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/java/demo1/src/main/java/com/whylogs/examples/WhyLogsDemo.java)</li></ul> |
| Docker | Run whylogs as in Docker | <ul><li>[Rest Container](https://docs.whylabs.ai/docs/integrations-rest-container)</li></ul>|
| AWS S3 | Store whylogs profiles in S3 | <ul><li>[S3 example](https://github.com/whylabs/whylogs/blob/mainline/python/examples/integrations/Writing_Profiles.ipynb)</li></ul>

<!--| Kafka | Log and monitor Kafka topics with whylogs| <ul><li>[Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/python/Kafka.ipynb)</li><li> [Integrating whylogs into your Kafka ML Pipeline](https://whylabs.ai/blog/posts/integrating-whylogs-into-your-kafka-ml-pipeline) </li></ul>|-->
<!--| Github actions | Unit test data with whylogs and github actions| <ul><li>[Notebook Example](https://github.com/whylabs/whylogs-examples/tree/mainline/github-actions)</li></ul> |-->
<!--| RAPIDS | Use whylogs in RAPIDS environment | <ul><li>[Notebook Example](https://github.com/whylabs/whylogs-examples/blob/mainline/python/RAPIDS%20GPU%20Integration%20Example.ipynb)</li><li>[Monitoring High-Performance Machine Learning Models with RAPIDS and whylogs](https://whylabs.ai/blog/posts/monitoring-high-performance-machine-learning-models-with-rapids-and-whylogs)</li></ul> |-->

## Examples

For a full set of our examples, please check out the [examples folder](/examples/).

Check out our example notebooks with Binder: [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/whylabs/whylogs-examples/HEAD)

- [Getting Started notebook](https://github.com/whylabs/whylogs-examples/blob/mainline/python/GettingStarted.ipynb)
- [Logging Example notebook](https://github.com/whylabs/whylogs-examples/blob/mainline/python/logging_example.ipynb)
- [Logging Images](https://github.com/whylabs/whylogs-examples/blob/mainline/python/Logging_Images.ipynb)
- [MLflow Integration](https://github.com/whylabs/whylogs-examples/blob/mainline/python/MLFlow%20Integration%20Example.ipynb)

## Usage Statistics<a name="whylogs-profiles" />

Starting with whylogs v1.0.0, whylogs collects anonymous information about a user’s environment. These usage statistics do not include any information about the user or the data that they are profiling, only the environment that the user in which the user is running whylogs.

To read more about what usage statistics whylogs collects, check out the relevant [documentation](https://docs.whylabs.ai/docs/usage-statistics/).

To turn off Usage Statistics, simply set the `WHYLOGS_NO_ANALYTICS` environment variable to True, like so:

```python
import os
os.environ['WHYLOGS_NO_ANALYTICS']='True'
```

## Community

If you have any questions, comments, or just want to hang out with us, please join [our Slack channel](http://join.slack.whylabs.ai/).

## Contribute

### How to Contribute

We welcome contributions to whylogs. Please see our [contribution guide](https://github.com/whylabs/whylogs/blob/mainline/CONTRIBUTING.md) and our [development guide](https://github.com/whylabs/whylogs/blob/mainline/DEVELOPMENT.md) for details.

### Contributors

<a href="https://github.com/whylabs/whylogs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=whylabs/whylogs" />
</a>

Made with [contrib.rocks](https://contrib.rocks).
