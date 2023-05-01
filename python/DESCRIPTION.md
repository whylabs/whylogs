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

## Quickstart

Install whylogs using the pip package manager in a terminal by running:

```
pip install whylogs
```

Then you can log data in python as simply as this:

```python
import whylogs as why
import pandas as pd

df = pd.read_csv("path/to/file.csv")
results = why.log(df)
```

And voilà, you now have a whylogs profile. To learn more about what a whylogs profile is and what you can do with it, check out our [docs](https://whylogs.readthedocs.io/en/latest/) and our [examples](https://github.com/whylabs/whylogs/tree/mainline/python/examples).
