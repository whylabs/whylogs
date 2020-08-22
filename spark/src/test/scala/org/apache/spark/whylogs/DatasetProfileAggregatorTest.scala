package org.apache.spark.whylogs

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.whylogs.core.message.InferredType
import org.scalatest.funsuite.AnyFunSuite

case class ExamplePoint(x: Int, y: Double, z: String, ts: Timestamp = new Timestamp(Instant.now().toEpochMilli))

class DatasetProfileAggregatorTest extends AnyFunSuite with SharedSparkContext {
  test("dataset profile aggregator with select() succeeds") {
    val _spark = spark
    import _spark.implicits._
    val numberOfEntries = 1000 * 1000

    val examples = (1 to numberOfEntries)
      .map(i => ExamplePoint(i, i * 1.50, "text " + i))
      .toDS()
      .repartition(16) // repartitioning forces Spark to serialize/deserialize data
      .toDF() // have to convert to a DataFrame, aka Dataset[Row]
      .repartition(16)

    // run the aggregation
    val dpAggregator = DatasetProfileAggregator("test", Instant.now().toEpochMilli)
    val profiles = examples.select(dpAggregator.toColumn).collect()
    assert(profiles.length == 1)

    val summary = profiles(0).value.toSummary
    assert(summary.getColumnsMap.size() == 4)

    // assert count
    assert(summary.getColumnsMap.get("x").getCounters.getCount == numberOfEntries)
    assert(summary.getColumnsMap.get("y").getCounters.getCount == numberOfEntries)
    assert(summary.getColumnsMap.get("z").getCounters.getCount == numberOfEntries)

    // assert various data type count
    assert(summary.getColumnsMap.get("x").getSchema.getInferredType.getType == InferredType.Type.INTEGRAL)
    assert(summary.getColumnsMap.get("y").getSchema.getInferredType.getType == InferredType.Type.FRACTIONAL)
    assert(summary.getColumnsMap.get("z").getSchema.getInferredType.getType == InferredType.Type.STRING)
  }

  test("dataset profile aggregator with groupBy().agg() succeeds") {
    val _spark = spark
    import _spark.implicits._
    val numberOfEntries = 1000 * 1000

    val examples = (1 to numberOfEntries)
      // the "x" field value is between 0 to 3 (inclusive)
      .map(i => ExamplePoint(i % 4, i * 1.50, "text " + i))
      .toDF() // have to convert to a DataFrame, aka Dataset[Row]
      .repartition(16)

    import org.apache.spark.sql.functions._

    // group by column x and aggregate
    val dpAggregator = DatasetProfileAggregator("test", Instant.now().toEpochMilli, groupByColumns = Seq("x"))
    val groupedDf = examples.groupBy(col("x"))
      .agg(dpAggregator.toColumn.name("whylogs_profile"))
    groupedDf.printSchema()

    // extract the nested column, collect them and turn them into Summary objects
    val summaries = groupedDf.select("whylogs_profile.value")
      .collect()
      .map(_.getAs[ScalaDatasetProfile](0))
      .map(_.value)
      .map(_.toSummary)
    assert(summaries.length == 4)

    // assert that the "x" column was not profiled
    for (summary <- summaries) {
      assert(!summary.containsColumns("x"))
    }

    // total number of counts should be the total number of entries
    assert(summaries.map(_.getColumnsOrThrow("y")).map(_.getCounters.getCount).sum == numberOfEntries)
    assert(summaries.map(_.getColumnsOrThrow("z")).map(_.getCounters.getCount).sum == numberOfEntries)
  }


  test("dataset profile aggregator works with SQL time") {
    val _spark = spark
    import _spark.implicits._
    val numberOfEntries = 1000 * 1000

    val examples = (1 to numberOfEntries)
      // the "x" field value is between 0 to 3 (inclusive)
      .map(i => {
        val dayTs = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(i % 3, ChronoUnit.DAYS)
        ExamplePoint(i % 4, i * 1.50, "text " + i, new Timestamp(dayTs.toEpochMilli))
      })
      .toDF() // have to convert to a DataFrame, aka Dataset[Row]
      .repartition(16)

    import org.apache.spark.sql.functions._

    // group by column x and aggregate
    val dpAggregator = DatasetProfileAggregator("test", Instant.now().toEpochMilli, timeColumn = "ts", groupByColumns = Seq("x"))

    // the dataset must be groupped by BOTH ts and x
    val groupedDf = examples.groupBy(col("x"), col("ts"))
      .agg(dpAggregator.toColumn.name("whylogs_profile"))
    groupedDf.printSchema()

    groupedDf.explain()

    // extract the nested column, collect them and turn them into Summary objects
    val summaries = groupedDf.select("whylogs_profile.value")
      .collect()
      .map(_.getAs[ScalaDatasetProfile](0))
      .map(_.value)
      .map(_.toSummary)
    assert(summaries.length == 12)

    // assert that the "x" column was not profiled
    for (summary <- summaries) {
      assert(!summary.containsColumns("x"))
    }

    // total number of counts should be the total number of entries
    assert(summaries.map(_.getColumnsOrThrow("y")).map(_.getCounters.getCount).sum == numberOfEntries)
    assert(summaries.map(_.getColumnsOrThrow("z")).map(_.getCounters.getCount).sum == numberOfEntries)
  }
}
