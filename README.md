## WhyLogs Java Library
[![license](https://img.shields.io/github/license/whylabs/whylogs-java)](https://github.com/whylabs/whylogs-java/blob/mainline/LICENSE)
[![javadoc](https://javadoc.io/badge2/ai.whylabs/whylogs-core/javadoc.svg)](https://javadoc.io/doc/ai.whylabs/whylogs-core)
[![openjdk](https://img.shields.io/badge/opendjk-%3E=1.8-green)](https://openjdk.java.net)

This is a Java implementation of WhyLogs, with support for Apache Spark integration for large scale datasets. The Python implementation can be found [here](https://github.com/whylabs/whylogs-python).

Understanding the properties of data as it moves through applications is essential to keeping your ML/AI pipeline stable and improving your user experience, whether your pipeline is built for production or experimentation. WhyLogs is an open source statistical logging library that allows data science and ML teams to effortlessly profile ML/AI pipelines and applications, producing log files that can be used for monitoring, alerts, analytics, and error analysis.

WhyLogs calculates approximate statistics for datasets of any size up to TB-scale, making it easy for users to identify changes in the statistical properties of a model's inputs or outputs. Using approximate statistics allows the package to run on minimal infrastructure and monitor an entire dataset, rather than miss outliers and other anomalies by only using a sample of the data to calculate statistics. These qualities make WhyLogs an excellent solution for profiling production ML/AI pipelines that operate on TB-scale data and with enterprise SLAs.

## Key Features

* **Data Insight:** WhyLogs provides complex statistics across different stages of your ML/AI pipelines and applications.

* **Scalability:** WhyLogs scales with your system, from local development mode to live production systems in multi-node clusters, and works well with batch and streaming architectures.

* **Lightweight:** Lightweight: WhyLogs produces small mergeable lightweight outputs in a variety of formats, using sketching algorithms and summarizing statistics.

* **Unified data instrumentation:** To enable data engineering pipelines and ML pipelines to share a common framework for tracking data quality and drifts, the WhyLogs library supports multiple languages and integrations.
  
* **Observability:** In addition to supporting traditional monitoring approaches, WhyLogs data can support advanced ML-focused analytics, error analysis, and data quality and data drift detection.

## Usage

To get started, add WhyLogs to your Maven POM:
```xml
<dependency>
  <groupId>ai.whylabs</groupId>
  <artifactId>whylogs-core</artifactId>
  <version>0.0.2b2</version>
</dependency>
```
For the full Java API signature, see the [Java Documentation](https://www.javadoc.io/doc/ai.whylabs/whylogs-core/latest/index.html).

Spark package (Scala 2.11 or 2.12 only):
```xml
<dependency>
  <groupId>ai.whylabs</groupId>
  <artifactId>whylogs-spark_2.11</artifactId>
  <version>0.0.2b2</version>
</dependency>
```
For the full Scala API signature, see the [Scala API Documentation](https://javadoc.io/doc/ai.whylabs/whylogs-spark_2.11/latest/index.html).


### Simple tracking
The following code is a simple tracking example that does not output data to disk:

```java
import com.whylogs.core.DatasetProfile;
import java.time.Instant;
import java.util.HashMap;
import com.google.common.collect.ImmutableMap;

public class Demo {
    public void demo() {
        final List<String> tags = ImmutableMap.of("tag", "tagValue");
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

### Serialization and deserialization
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
### Merging dataset profiles
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
### Apache Spark integration
```scala
import org.apache.spark.sql.functions._
// implicit import for WhyLogs to enable
import com.whylogs.spark.WhyLogs._

val raw_df = spark.read.option("header", "true").csv("/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv")
val df = raw_df.withColumn("call_date", to_timestamp(col("Call Date"), "MM/dd/YYYY"))

val profiles = df.newProfilingSession("FireDepartment")
  .withTimeColumn("call_date") // split dataset by call_date
  .groupBy("Zipcode of Incident")
  .aggProfiles() // returns a dataframe of <timestamp, datasetProfile> entries

```
For further analysis, dataframes can be stored in a Parquet file, or collected to the driver if the number of entries is small enough.

## Building and Testing
* To build, run `./gradlew build`
* To test, run `./gradlew test`
