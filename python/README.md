<img src="https://static.scarf.sh/a.png?x-pxid=bc3c57b0-9a65-49fe-b8ea-f711c4d35b82" /><p align="center">
<img src="https://i.imgur.com/nv33goV.png" width="35%"/>
</br>

<h1 align="center">The open standard for data logging

 </h1>
  <h3 align="center">
   <a href="https://whylogs.readthedocs.io/"><b>Documentation</b></a> &bull;
   <a href="https://bit.ly/whylogsslack"><b>Slack Community</b></a> &bull;
   <a href="https://github.com/whylabs/whylogs#python-quickstart"><b>Python Quickstart</b></a> &bull;
   <a href="https://whylogs.readthedocs.io/en/latest/examples/integrations/writers/Writing_to_WhyLabs.html"><b>WhyLabs Quickstart</b></a>
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

<a href="https://hub.whylabsapp.com/signup" target="_blank">
    <img src="https://user-images.githubusercontent.com/7946482/193939278-66a36574-2f2c-482a-9811-ad4479f0aafe.png" alt="WhyLabs Signup">
</a>

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

And voilà, you now have a whylogs profile. To learn more about about a whylogs profile is and what you can do with it, read on.

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

## WhyLabs<a name="whylabs" />

WhyLabs is a managed service offering built for helping users make the most of their whylogs profiles. With WhyLabs, users can ingest profiles and set up automated monitoring as well as gain full observability into their data and ML systems. With WhyLabs, users can ensure the reliability of their data and models, and debug any problems that arise with them.

Ingesting whylogs profiles into WhyLabs is easy. After obtaining your access credentials from the platform, you’ll need to set them in your Python environment, log a dataset, and write it to WhyLabs, like so:

```python
import whylogs as why
import os

os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-0" # ORG-ID is case-sensitive
os.environ["WHYLABS_API_KEY"] = "YOUR-API-KEY"
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = "model-0" # The selected model project "MODEL-NAME" is "model-0"

results = why.log(df)

results.writer("whylabs").write()
```

![image](<https://github.com/whylabs/whylogs/blob/assets/images/chrome-capture-2022-9-4%20(1).gif>)

If you’re interested in trying out WhyLabs, check out the always free [Starter edition](https://hub.whylabsapp.com/signup), which allows you to experience the entire platform’s capabilities with no credit card required.

## Data Constraints<a name="data-constraints" />

Constraints are a powerful feature built on top of whylogs profiles that enable you to quickly and easily validate that your data looks the way that it should. There are numerous types of constraints that you can set on your data (that numerical data will always fall within a certain range, that text data will always be in a JSON format, etc) and, if your dataset fails to satisfy a constraint, you can fail your unit tests or your CI/CD pipeline.

A simple example of setting and testing a constraint is:

```python
import whylogs as why
from whylogs.core.constraints import Constraints, ConstraintsBuilder
from whylogs.core.constraints.factories import greater_than_number

profile_view = why.log(df).view()
builder = ConstraintsBuilder(profile_view)
builder.add_constraint(greater_than_number(column_name="col_name", number=0.15))

constraints = builder.build()
constraints.report()
```

To learn more about constraints, check out: the [Constraints Example](https://bit.ly/whylogsconstraintsexample).

## Profile Visualization<a name="profile-visualization" />

In addition to being able to automatically get notified about potential issues in data, it’s also useful to be able to inspect your data manually. With the profile visualizer, you can generate interactive reports about your profiles (either a single profile or comparing profiles against each other) directly in your Jupyter notebook environment. This enables exploratory data analysis, data drift detection, and data observability.

To access the profile visualizer, install the `[viz]` module of whylogs by running `pip install "whylogs[viz]"` in your terminal. One type of profile visualization that we can create is a drift report; here's a simple example of how to analyze the drift between two profiles:

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

whylogs can seamslessly interact with different tooling along your Data and ML pipelines. We have currently built integrations with:

- AWS S3
- Apache Airflow
- Apache Spark
- Mlflow
- GCS

and much more!

If you want to check out our complete list, please refer to our [integrations examples](https://github.com/whylabs/whylogs/tree/mainline/python/examples/integrations) page.

## Examples

For a full set of our examples, please check out the [examples folder](https://github.com/whylabs/whylogs/tree/mainline/python/examples).

## Usage Statistics<a name="whylogs-profiles" />

Starting with whylogs v1.0.0, whylogs by default collects anonymous information about a user’s environment. These usage statistics do not include any information about the user or the data that they are profiling, only the environment that the user in which the user is running whylogs.

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

We welcome contributions to whylogs. Please see our [contribution guide](https://github.com/whylabs/whylogs/blob/mainline/.github/CONTRIBUTING.md) and our [development guide](https://github.com/whylabs/whylogs/blob/mainline/.github/DEVELOPMENT.md) for details.

### Contributors

<a href="https://github.com/whylabs/whylogs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=whylabs/whylogs" />
</a>

Made with [contrib.rocks](https://contrib.rocks).
