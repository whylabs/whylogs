# Basics - v0 to v1

whylogs v1 has been released on **May 31st, 2022**. While this release represents an exciting milestone for the whyogs library, there are some important implications for current users of whylogs v0. Most notably, whylogs v1 comes with a simplified and more intuitive API. This means that if you choose to upgrade to v1, code changes will be required.

Users can visit this [migration guide](https://docs.whylabs.ai/docs/v1-migration/) to assist with migrating their code to whylogs v1.

## What’s New With whylogs v1


Based on user feedback, the following areas of improvement have been the focus for whylogs v1.

### Performance Improvements

Users will see substantial improvements to performance, allowing larger datasets to be profiled in much less time.

### API Simplification

Previously, profiling a dataset involved initializing a session, creating a logger within that session, and calling a logging function. With whylogs v1, this process is simplified and made more intuitive by simplifying the number of entities you need to configure at first.

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
