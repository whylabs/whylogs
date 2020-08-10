.. _overview:

===================================
Overview
===================================

Introduction
===================================

WhyLogs (https://github.com/whylabs/whylogs) is an open source data quality \
library for logging and monitoring data for your AI/ML application with advanced \
data science statistics. WhyLogs is designed for scaling with your MLOps workflow, \
and can scale from local development to production terabyte-size datasets.


Whether you are running an experimentation or production pipeline, understanding the \
properties of data that flows through the application is critical for the success of \
the ML project. WhyLogs enables advanced statistical collection using lightweight techniques \
(i.e. building sketches for data) that can enable complex monitoring and data quality check for your \
pipeline.

Benefits
===================================

* **Data Insight** provides complex statistics across different stages of your pipelines.

* **Scalability** scales with your system, from local development mode to live production system in multi-node clusters.

* **Lightweight** using sketching algorithms and summarization statistics, WhyLogs produces small mergeable lightweight
  outputs that can be used for local analysis.

* **Advanced monitoring** on top of supporting traditional monitoring approaches, WhyLogs data can support advanced ML-focused
  data quality and monitoring checks such as drift detection, data type distribution, histogram shifts.

* **Unified data monitoring** enable both data engineering pipelines and ML pipelines to share a common framework
  for monitoring data quality and drifts.
