package com.whylogs.spark

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

import org.apache.commons.lang3.RandomUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.whylogs.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.io.Directory

case class TestDataPoint(x: String, i: Int, d: Double, ts: Timestamp) extends Serializable

case class TestPrediction(prediction: Int, target: Int, score: Double) extends Serializable

class WhyLogsTest extends AnyFunSuite with SharedSparkContext {
  test("test WhyLogsSession") {
    import com.whylogs.spark.WhyLogs._

    val _spark = spark
    import _spark.sqlContext.implicits._
    val today = Instant.now().truncatedTo(ChronoUnit.DAYS)

    val df = (1 to 10000)
      .map(i => {
        val epochMillis = today.minus(i % 3, ChronoUnit.DAYS).toEpochMilli
        val name = "TestData-" + i % 2
        TestDataPoint(name, i, RandomUtils.nextDouble(0, 100), new Timestamp(epochMillis))
      })
      .toDF()
      .repartition(32)
    df.persist()
    df.count()

    val profiles = df.newProfilingSession("foobar")
      .withTimeColumn("ts")
      .groupBy("x").aggProfiles()
    val count = profiles.count()
    assert(count == 6)

    // verify that we can read and write t
    val tmpDir = Directory.makeTemp("whylogs")
    try {
      profiles.write.mode(SaveMode.Overwrite).parquet(tmpDir.toURI.toString)
      // read the data back and print to stdout
      spark.read.parquet(tmpDir.toURI.toString).show(truncate = false)
    } finally {
      try {
        tmpDir.deleteRecursively()
      } catch {
        case _: Exception => // do nothing
      }
    }
  }

  test("test WhyLogsSession with ClassificationMetrics") {
    import com.whylogs.spark.WhyLogs._

    val df = spark.read.parquet("file:///tmp/data.parquet")
    val res = df.newProfilingSession("model")
        .withModelProfile("predicted", "target", "score")
        .aggProfiles(Instant.now())
    res.count()
    res.printSchema()
  }

}
