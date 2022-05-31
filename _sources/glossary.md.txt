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

#### Dataset
**Dataset** is a collection of related data that will be analyzed together. In case of tabular data: each column of
the table represents a particular variable, and each row represents a record of the dataset. When used alongside a
statistical model, the dataset often represents features as columns, with additional columns for the output. For
non-structured/complex data, the representation depends on the datatype, explore other data
types [here](https://docs.whylabs.ai/docs/whylabs-monitoring/#discrete-vs-continuous-columns).

#### DatasetProfile
A **DatasetProfile** is a collection of summary statistics and related metadata for a dataset that whylogs has
processed.

#### Data Sketching
**Data sketching** is a class of algorithms that efficiently extract information from large or streaming datasets in a
single pass. This term is sometimes used to refer specifically to the Apache DataSketches project.

#### Logger
A **logger** represents the whylogs tracking object for a given dataset (in batch mode) or a collection of data points (
in streaming mode). A logger is always associated with a timestamp for its creation and a timestamp for the dataset.
Different loggers may write to different storage systems using different output formats.

#### Metadata
**Metadata** is data that describes either a dataset or information from whylogs’ processing of the dataset.

#### whylogs file
[NEED UPDATING] The **whylogs output** is available in the following formats: protobuf, JSON, and flat. Protobuf is a lightweight binary
format that maps one-to-one with the memory representation of a whylogs object. JSON displays the protobuf data in JSON
format. Flat outputs multiple files with both CSV and JSON content to represent different views of the data, including
histograms, upper-bound, lower-bound, and frequent values. To apply advanced transformation on whylogs, we recommend
using Protobuf.

#### Metric
TBD New world

##### Metric component
NEED TO DEFINE HERE
