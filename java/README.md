# Version upgrade to 1.0.0 coming soon!
We're excited to announce that v1 of the API to the Java side is coming soon! You can look forward to:
1. Simpler API that gets you started quickly.
2. Constraints on the Metrics
3. Updated ProfileVisualizer
4. Refresh for Usability

For now, follow this readme to get started in v0, and check back here for updates. If you'd like to watch the development unfold or contribute, check out the repo out [here](https://github.com/whylabs/whylogs/tree/mainline/java)

# WhyLogs Java Library
[![license](https://img.shields.io/github/license/whylabs/whylogs-java)](https://github.com/whylabs/whylogs-java/blob/mainline/LICENSE)
[![javadoc](https://javadoc.io/badge2/ai.whylabs/whylogs-java-core/javadoc.svg)](https://javadoc.io/doc/ai.whylabs/whylogs-java-core)
[![openjdk](https://img.shields.io/badge/opendjk-%3E=1.8-green)](https://openjdk.java.net)

This is a Java implementation of WhyLogs v0 with support for Apache Spark integration for large scale datasets which can be seen [here](https://github.com/whylabs/whylogs/tree/maintenance/0.7.x/java). The Python implementation can be found [here](https://github.com/whylabs/whylogs-python).

Understanding the properties of data as it moves through applications is essential to keeping your ML/AI pipeline stable and improving your user experience, whether your pipeline is built for production or experimentation. WhyLogs is an open source statistical logging library that allows data science and ML teams to effortlessly profile ML/AI pipelines and applications, producing log files that can be used for monitoring, alerts, analytics, and error analysis.

WhyLogs calculates approximate statistics for datasets of any size up to TB-scale, making it easy for users to identify changes in the statistical properties of a model's inputs or outputs. Using approximate statistics allows the package to run on minimal infrastructure and monitor an entire dataset, rather than miss outliers and other anomalies by only using a sample of the data to calculate statistics. These qualities make WhyLogs an excellent solution for profiling production ML/AI pipelines that operate on TB-scale data and with enterprise SLAs.

# Key Features

* **Data Insight:** WhyLogs provides complex statistics across different stages of your ML/AI pipelines and applications.

* **Scalability:** WhyLogs scales with your system, from local development mode to live production systems in multi-node clusters, and works well with batch and streaming architectures.

* **Lightweight:** WhyLogs produces small mergeable lightweight outputs in a variety of formats, using sketching algorithms and summarizing statistics.

* **Unified data instrumentation:** To enable data engineering pipelines and ML pipelines to share a common framework for tracking data quality and drifts, the WhyLogs library supports multiple languages and integrations.

* **Observability:** In addition to supporting traditional monitoring approaches, WhyLogs data can support advanced ML-focused analytics, error analysis, and data quality and data drift detection.

## Glossary/Concepts
**Project:** A collection of related data sets used for multiple models or applications.

**Pipeline:** One or more datasets used to build a single model or application. A project may contain multiple pipelines.

**Dataset:** A collection of records. WhyLogs v0.0.2 supports structured datasets, which represent data as a table where each row is a different record and each column is a feature of the record.

**Feature:** In the context of WhyLogs v0.0.2 and structured data, a feature is a column in a dataset. A feature can be discrete (like gender or eye color) or continuous (like age or salary).

**WhyLogs Output:** WhyLogs returns profile summary files for a dataset in JSON format. For convenience, these files are provided in flat table, histogram, and frequency formats.

**Statistical Profile:** A collection of statistical properties of a feature. Properties can be different for discrete and continuous features.

## Statistical Profile
WhyLogs collects approximate statistics and sketches of data on a column-basis into a statistical profile. These metrics include:
* **Simple counters**: boolean, null values, data types.
* **Summary statistics**: sum, min, max, variance.
* **Unique value counter** or **cardinality**: tracks an approximate unique value of your feature using HyperLogLog algorithm.
* **Histograms** for numerical features. WhyLogs binary output can be queried to with dynamic binning based on the shape of your data.
* **Top frequent items** (default is 128). Note that this configuration affects the memory footprint, especially for text features.

## Performance
We tested WhyLogs Java performance on the following datasets to validate WhyLogs memory footprint and the output binary.

* Lending Club Data: [Kaggle Link](https://www.kaggle.com/wordsforthewise/lending-club)
* NYC Tickets: [Kaggle Link](https://www.kaggle.com/new-york-city/nyc-parking-tickets)
* Pain Pills in the USA: [Kaggle Link](https://www.kaggle.com/paultimothymooney/pain-pills-in-the-usa)

We ran our profile (in `cli` sub-module in this package) on each the dataset and collected JMX metrics.

|        Dataset        | Size  | No. of Entries | No. of Features | Est. Memory Consumption | Output Size (uncompressed) |
|:---------------------:|-------|----------------|-----------------|-------------------------|-----------------------|
| Lending Club          | 1.6GB | 2.2M           | 151             | 14MB                    | 7.4MB                  |
| NYC Tickets           | 1.9GB | 10.8M          | 43              | 14MB                    | 2.3MB                 |
| Pain pills in the USA | 75GB  | 178M           | 42              | 15MB                    | 2MB                   |

# Usage

To get started, add WhyLogs to your Maven POM:
```xml
<dependency>
  <groupId>ai.whylabs</groupId>
  <artifactId>whylogs-core</artifactId>
  <version>0.1.0</version>
</dependency>
```
For the full Java API signature, see the [Java Documentation](https://www.javadoc.io/doc/ai.whylabs/whylogs-core/latest/index.html).

Spark package (Scala 2.11 or 2.12 only):
```xml
<dependency>
  <groupId>ai.whylabs</groupId>
  <artifactId>whylogs-spark_2.11</artifactId>
  <version>0.1.0</version>
</dependency>
```
For the full Scala API signature, see the [Scala API Documentation](https://javadoc.io/doc/ai.whylabs/whylogs-spark_2.11/latest/index.html).

## Examples Repo

For examples in different languages, please checkout our [whylogs-examples](https://github.com/whylabs/whylogs-examples) repository.

## Simple tracking
The following code is a simple tracking example that does not output data to disk:

```java
import com.whylogs.core.DatasetProfile;
import java.time.Instant;
import java.util.HashMap;
import com.google.common.collect.ImmutableMap;

public class Demo {
    public void demo() {
        final Map<String, String> tags = ImmutableMap.of("tag", "tagValue");
        final DatasetProfile profile = new DatasetProfile("test-session", Instant.now(), tags);
        profile.track("my_feature", 1);
        profile.track("my_feature", "stringValue");
        profile.track("my_feature", 1.0);

        final HashMap<String, Object> dataMap = new HashMap<>();
        dataMap.put("feature_1", 1);
        dataMap.put("feature_2", "text");
        dataMap.put("double_type_feature", 3.0);
        profile.track(dataMap);
    }
}
```

## Serialization and deserialization
WhyLogs uses Protobuf as the backing storage format. To write the data to disk, use the standard Protobuf
serialization API as follows.

```java
import com.whylogs.core.DatasetProfile;
import java.io.InputStream;import java.nio.file.Files;
import java.io.OutputStream;
import java.nio.file.Paths;
import com.whylogs.core.message.DatasetProfileMessage;

class SerializationDemo {
    public void demo(DatasetProfile profile) {
        try (final OutputStream fos = Files.newOutputStream(Paths.get("profile.bin"))) {
            profile.toProtobuf().build().writeDelimitedTo(fos);
        }
        try (final InputStream is = new FileInputStream("profile.bin")) {
            final DatasetProfileMessage msg = DatasetProfileMessage.parseDelimitedFrom(is);
            final DatasetProfile profile = DatasetProfile.fromProtobuf(msg);

            // continue tracking
            profile.track("feature_1", 1);
        }

    }
}
```
## Merging dataset profiles
In enterprise systems, data is often partitioned across multiple machines for distributed processing. Online systems may also process data on multiple machines, requiring engineers to run ad-hoc analysis using an ETL-based system to build complex metrics, such as counting unique visitors to a website.

WhyLogs resolves this by allowing users to merge sketches from different machines. To merge two WhyLogs
`DatasetProfile` files, those files must:
* Have the same name
* Have the same session ID
* Have the same data timestamp
* Have the same tags

The following is an example of the code for merging files that meet these requirements.

```java
import com.whylogs.core.DatasetProfile;
import java.io.InputStream;import java.nio.file.Files;
import java.io.OutputStream;
import java.nio.file.Paths;
import com.whylogs.core.message.DatasetProfileMessage;

class SerializationDemo {
    public void demo(DatasetProfile profile) {
        try (final InputStream is1 = new FileInputStream("profile1.bin");
                final InputStream is2 = new FileInputStream("profile2.bin")) {
            final DatasetProfileMessage msg = DatasetProfileMessage.parseDelimitedFrom(is);
            final DatasetProfile profile1 = DatasetProfile.fromProtobuf(DatasetProfileMessage.parseDelimitedFrom(is1));
            final DatasetProfile profile2 = DatasetProfile.fromProtobuf(DatasetProfileMessage.parseDelimitedFrom(is2));

            // merge
            profile1.merge(profile2);
        }

    }
}
```
## Apache Spark integration

Our integration is compatible with Apache Spark 2.x (3.0 support is to come).

This example shows how we use WhyLogs to profile a dataset based on time and categorical information. The data is from the
public dataset for [Fire Department Calls & Incident](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4338404698899132/4435723924568266/2419783655524824/latest.html).

```scala
import org.apache.spark.sql.functions._
// implicit import for WhyLogs to enable newProfilingSession API
import com.whylogs.spark.WhyLogs._

// load the data
val raw_df = spark.read.option("header", "true").csv("/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv")
val df = raw_df.withColumn("call_date", to_timestamp(col("Call Date"), "MM/dd/YYYY"))

val profiles = df.newProfilingSession("profilingSession") // start a new WhyLogs profiling job
                 .withTimeColumn("call_date") // split dataset by call_date
                 .groupBy("City", "Priority") // tag and group the data with categorical information
                 .aggProfiles() //  runs the aggregation. returns a dataframe of <timestamp, datasetProfile> entries


```
For further analysis, dataframes can be stored in a Parquet file, or collected to the driver if the number of entries is small enough.

## Building and Testing

* To build, run `./gradlew build`
* To test, run `./gradlew test`
