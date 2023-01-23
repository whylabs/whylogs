# Basics - v0 to v1

whylogs v1 has been released on **May 31st, 2022**. While this release represents an exciting milestone for the whyogs library, there are some important implications for current users of whylogs v0. Most notably, whylogs v1 comes with a simplified and more intuitive API. This means that if you choose to upgrade to v1, code changes will be required.

Users can visit this [migration guide](https://docs.whylabs.ai/docs/v1-migration/) to assist with migrating their code to whylogs v1.

If you need to convert a serialized v0 profile to a v1 profile, please check our [v0-to-v1 converter example](https://nbviewer.org/github/whylabs/whylogs/blob/mainline/python/examples/advanced/converting_v0_to_v1.ipynb).

## What’s New With whylogs v1


Based on user feedback, the following areas of improvement have been the focus for whylogs v1.

### Performance Improvements

One of the major causes for performance improvements with whylogs v1 is a change from row-level operations to columnar operations. Columnar operations allow us to take advantage of [vectorization](https://www.pythonlikeyoumeanit.com/Module3_IntroducingNumpy/VectorizedOperations.html#Vectorized-Operations) built into the NumPy and pandas packages, which significantly speeds up the process of generating profiles by pushing summarization from slow Python code to lightning-fast C code. Importantly, we can take advantage of vectorization without changes to the end user experience.

### API Simplification

In whylogs v1, generating whylogs profile takes a single line of code: `results = why.log(pandas_df)`.

In the original whylogs v0 API, if you wanted to log a dataframe, you would need to start by initializing a session. Within that session, you would need to create a logger, and then, finally, within that logger call a log_dataframe() function. We heard from our users that these concepts were often confusing and slowed them down. The new simplified API enables users to  easily create whylogs profiles as artifacts to represent their datasets.

### Customizability

Users are now able to customize whylogs' behavior in different ways. You can select which metrics are to be tracked according to the column's type or column's name. In addition, whylogs v1 provides extension hooks, enabling the user to define your own customized metric and also define your own customized constraints for data validation.

### Profile Constraints

With whylogs v1, users can generate custom constraints on their dataset. For example, users can define a constraint requiring credit scores to be between 300 and 850, metric distributions, like mean, median or standard deviation to fall within a defined range or assert that a categorical column's frequent items belong to a defined set of categories.


### Profile Visualizer

With the profile visualizer, users can generate interactive reports about their profiles (either a single profile or comparing profiles against each other) directly in a Jupyter notebook environment. This enables exploratory data analysis, data drift detection, and data observability.

### Profile vs View

In whylogs v1, the concept of **Profile View** is introduced. The Profile View is obtained from a Profile object. The profile object should be used while logging operations are underway. Once logging is done for the desired set of data, you can inspect, visualize, merge and upload your profile through the Profile View object. When a profile is written to a binary file, it is converted to a Profile View. This separates both concepts according to their intended use: logging vs. inspection.

### Easier Configuration

In v0, config (yaml) files were used at various points throughout the project. In whylogs v1, configuration relies on user-provided parameters rather than config files.

### Usability Refresh

A top priority of the whylogs v1 project has been maximizing usability to ensure that users can get up and running with whylogs v1 quickly and easily. This usability refresh will include an updated GitHub readme, automatically generated documentation of whylogs v1 code, and an updated suite of examples and resources.
