## WhyLogs Java Library
[![license](https://img.shields.io/github/license/whylabs/whylogs-java)](https://github.com/whylabs/whylogs-java/blob/mainline/LICENSE)
[![javadoc](https://javadoc.io/badge2/ai.whylabs/whylogs-core/javadoc.svg)](https://javadoc.io/doc/ai.whylabs/whylogs-core)
[![openjdk](https://img.shields.io/badge/opendjk-%3E=1.8-green)](https://openjdk.java.net)

WhyLogs is the first of its kind statistical logging library. Whether you are running an experimentation or production ML/AI pipeline, understanding the properties of data that flows through an application is critical for stable model behavior and great customer experience. With WhyLogs, data science and ML teams can effortlessly instrument ML/AI pipelines and applications. Resulting log files are used for monitoring, alerting, analytics and error analysis.

WhyLogs is an open source package that calculates approximate statistics for datasets of any size (from small to TB-size) in order to identify changes in the statistical properties of model's inputs or outputs. Approximate statistics allows the package to be deployed with minimal infrastructure requirements, and to work with an entire dataset as opposed to calculating actual statistics on a small sample of data which may miss outliers and other anomalies. These qualities make WhyLogs an excellent solution for instrumenting production ML/AI pipelines that operate on TB-scale data and with enterprise SLAs.
 
This is a Java implementation of WhyLogs, with support for Apache Spark integration for large scale datasets.

Python version: [whylogs-python](https://github.com/whylabs/whylogs-python).

## Key Features

* **Data Insight** provides complex statistics across different stages of your ML/AI pipelines and applications.

* **Scalability** scales with your system, from local development mode to live production system in multi-node clusters. Suitable for batch and streaming architectures. 

* **Lightweight** using sketching algorithms and summarization statistics, WhyLogs produces small mergeable lightweight
  outputs in a variety of formats.

* **Unified data instrumentation** enable both data engineering pipelines and ML pipelines to share a common framework for tracking data quality and drifts. WhyLogs library supports multiple languages and many integrations. 
  
* **Observability** on top of supporting traditional monitoring approaches, WhyLogs data can support advanced ML-focused data quality and data drift detection, analytics and error analysis. 

## Usage


To get started, add WhyLogs to your Maven POM:
```xml
<dependency>
  <groupId>ai.whylabs</groupId>
  <artifactId>whylogs-core</artifactId>
  <version>0.0.2b1</version>
</dependency>
```
For full Java API signature, please refer to the [Java Documentation](https://www.javadoc.io/doc/ai.whylabs/whylogs-core/latest/index.html).

Spark package (Scala 2.11 or 2.12 only):
```xml
<dependency>
  <groupId>ai.whylabs</groupId>
  <artifactId>whylogs-spark_2.11</artifactId>
  <version>0.0.2b1</version>
</dependency>
```
For full Scala API signature, please refer to the [Scala API Documentation](https://javadoc.io/doc/ai.whylabs/whylogs-spark_2.11/latest/index.html).


### Simple tracking
A simple tracking example without outputing data to disk:

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
WhyLogs uses Protobuf as the backing storage format. To write the data to disk, you can use standard Protobuf
serialization API:

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
### Merging multiple dataset profiles
A common pattern in enterprise system is to partition your data across different machine for distributed processing. For
online system, data can also be processed independently on multiple machines, and in order to build complex metrics such
as counting unique visitors for a website, engineers have to flow data into an ETL-based system to run ad hoc analysis.

WhyLogs addresses this use cases by allowing users to merge your sketches across different machines. To merge two WhyLogs
`DatasetProfile` files, they must:
* Have the same name
* Have the same session IDs
* Have the same data timestamp
* Have the same tags

In that case, the merging code looks like this:

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
The dataframes can be stored in either Parquet file or collected to the driver (if the number of entries is small enough)
for further analysis.

## Building and Testing
* To build, run `./gradlew build`
* To test, run `./gradlew test`
