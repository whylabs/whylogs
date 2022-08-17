# Glossary

The following is a glossary of terms commonly used throughout the WhyLabs Documentation. Note that some of these terms
have a special meaning in the context of whylogs/WhyLabs.

##### whylogs
whylogs is a data logging library that captures statistical properties of data and ML models.

##### Batch
A batch is a collection of datapoints, often grouped by time.

###### Batch Mode
In batch mode, whylogs processes a dataset in batches.

###### Streaming Mode
In streaming mode, whylogs processes individual data points. However, the underlying algorith groups these
data points together into micro batches.

##### Column Profile
A collection of metrics that represents a feature/column in the dataset.

##### Constraints
Constraints are rules you create to assert that your data lies within the expected range. You can define a set of constraints to be applied to a given dataset profile in order to generate a report or a boolean check if any of the defined constrains failed to be met.

#### Dataset
**Dataset** is a collection of related data that will be analyzed together. In case of tabular data: each column of
the table represents a particular variable, and each row represents a record of the dataset. When used alongside a
statistical model, the dataset often represents features as columns, with additional columns for the output.

#### Dataset Profile
A **Dataset Profile** is a collection of summary statistics and related metadata for a dataset that whylogs has
processed. A **Dataset Profile** is a collection of Column Profiles.

#### Dataset Schema
A **Dataset Schema** defines the schema for tracking metrics in whylogs. In a dataset schema, the columns of the dataset are mapped to specific data types, and metrics are mapped to each data type. A **Dataset Schema** is created automatically when logging, but it can also be customized by the user, defining column types and/or metrics to be tracked.


#### Data Sketching
**Data sketching** is a class of algorithms that efficiently extract information from large or streaming datasets in a
single pass. This term is sometimes used to refer specifically to the Apache DataSketches project.

#### Metadata
**Metadata** is data that describes either a dataset or information from whylogs’ processing of the dataset.

#### Metric
The summary statistics of a dataset is composed by a collection of metrics. Metrics are defined according to its namespaces. Some of the whylogs' default namespaces are: `counts`, `types`, `distribution`, `cardinality`, `frequent items/frequent strings`. By default, a column of a given data type will have a defined set of metrics assigned to it. The metrics to be tracked are customizable according to data type or column name (see [Schema Configuration](https://github.com/whylabs/whylogs/blob/mainline/python/examples/basic/Schema_Configuration.ipynb)). New metrics can also be created in a custom fashion, according to the need of the user (see TBD).


##### Metric component
A metric component is the smallest unit for a metric.A metric might consist of multiple components. An example is ints metric, which consists of min and max metric components. The calculation of components could be independent or could be coupled with other components.


#### whylogs file

A whylogs-generated profile can be stored as a protobuf binary file. [Protobuf](https://developers.google.com/protocol-buffers) stands for a serialization technology called protocol buffer, and is a lightweight binary format that maps one-to-one with the memory representation of a whylogs object.
