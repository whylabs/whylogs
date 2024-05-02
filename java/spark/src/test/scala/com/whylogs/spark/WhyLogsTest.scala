package com.whylogs.spark

import java.io.ByteArrayInputStream
import java.nio.file.{Files, StandardCopyOption}
import java.sql.Timestamp
import java.time.{Instant, ZonedDateTime}
import java.time.temporal.ChronoUnit
import com.whylogs.core.DatasetProfile
import com.whylogs.core.message.InferredType
import com.whylogs.spark.WhyLogs.ProfiledDataFrame
import org.apache.commons.lang3.RandomUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.whylogs.SharedSparkContext

import org.scalatest.exceptions.TestFailedException
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.reflect.io.Directory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class TestDataPoint(x: String, i: Int, d: Double, ts: Timestamp) extends Serializable

case class TestPrediction(prediction: Int, target: Int, score: Double) extends Serializable

class WhyLogsTest extends AnyFunSuite with SharedSparkContext {
  val ABS_TOL_MSG = " using absolute tolerance"
  private def AbsoluteErrorComparison(x: Double, y: Double, eps: Double): Boolean = {
    math.abs(x - y) < eps
  }

  case class CompareDoubleRightSide(
    fun: (Double, Double, Double) => Boolean, y: Double, eps: Double, method: String)

   implicit class DoubleWithAlmostEquals(val x: Double) {

    def ~=(r: CompareDoubleRightSide): Boolean = r.fun(x, r.y, r.eps)
    def !~=(r: CompareDoubleRightSide): Boolean = !r.fun(x, r.y, r.eps)

    def ~==(r: CompareDoubleRightSide): Boolean = {
      if (!r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Expected $x and ${r.y} to be within ${r.eps}${r.method}.", 0)
      }
      true
    }

    def !~==(r: CompareDoubleRightSide): Boolean = {
      if (r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Did not expect $x and ${r.y} to be within ${r.eps}${r.method}.", 0)
      }
      true
    }

    /**
     * Comparison using absolute tolerance.
     */
    def absTol(eps: Double): CompareDoubleRightSide =
      CompareDoubleRightSide(AbsoluteErrorComparison, x, eps, ABS_TOL_MSG)

    override def toString: String = x.toString
  }

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

    // verify that we can read and write t
    val tmpDir = Directory.makeTemp("whylogs")
    try {
      profiles.write.mode(SaveMode.Overwrite).parquet(tmpDir.toURI.toString)
      // read the data back and print to stdout
      spark.read.parquet(tmpDir.toURI.toString)
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

    val file = Files.createTempFile("data", ".parquet")
    Files.copy(WhyLogs.getClass.getResourceAsStream("/prediction_data.parquet"), file, StandardCopyOption.REPLACE_EXISTING)

    val df = spark.read.parquet("file://" + file.toAbsolutePath)
    val res = df.newProfilingSession("model")
      .withClassificationModel("predictions", "targets", "scores")
      .aggProfiles(Instant.now())
    res.count()
    val bytes = res.collect()(0).getAs[Array[Byte]](0)
    val dp = DatasetProfile.parse(new ByteArrayInputStream(bytes))

    assert(dp.getModelProfile != null)
    assert(dp.getModelProfile.getMetrics.getClassificationMetrics.getLabels == List("0", "1").asJava)
    val matrix: Array[Array[Long]] = dp.getModelProfile.getMetrics.getClassificationMetrics.getConfusionMatrix
    assert(matrix(0)(0) == 40L)
    assert(matrix(0)(1) == 7L)
    assert(matrix(1)(0) == 11L)
    assert(matrix(1)(1) == 42L)
  }

  test("test WhyLogsSession with ModelMetrics") {
    import com.whylogs.spark.WhyLogs._

    val file = Files.createTempFile("data", ".parquet")
    Files.copy(WhyLogs.getClass.getResourceAsStream("/prediction_data.parquet"), file, StandardCopyOption.REPLACE_EXISTING)

    val df = spark.read.parquet("file://" + file.toAbsolutePath)
    val res = df.newProfilingSession("model")
      .withRegressionModel("predictions", "targets")
      .aggProfiles(Instant.now())
    res.count()
    val bytes = res.collect()(0).getAs[Array[Byte]](0)
    val dp = DatasetProfile.parse(new ByteArrayInputStream(bytes))

    assert(dp.getModelProfile != null)
    assert(dp.getModelProfile != null)
    assert(dp.getModelProfile.getMetrics.getClassificationMetrics == null)
    assert(dp.getModelProfile.getMetrics.getRegressionMetrics != null)
  }

  test("test WhyLogs with RegressionMetrics") {
    val file = Files.createTempFile("data", ".parquet")
    Files.copy(WhyLogs.getClass.getResourceAsStream("/brazillian_608_features.parquet"), file, StandardCopyOption.REPLACE_EXISTING)

    val df = spark.read.parquet("file://" + file.toAbsolutePath)

    val res = df
      .withColumn("delivery_prediction", col("delivery_prediction").cast(IntegerType))
      .withColumn("delivery_status", col("delivery_status").cast(IntegerType))
      .newProfilingSession("model")
      .withRegressionModel("delivery_prediction", "delivery_status")
      .aggProfiles(Instant.now())
    res.count()

    val bytes = res.collect()(0).getAs[Array[Byte]](0)
    val dp = DatasetProfile.parse(new ByteArrayInputStream(bytes))

    assert(dp.getModelProfile != null)
    assert(dp.getModelProfile.getMetrics.getClassificationMetrics == null)
    assert(dp.getModelProfile.getMetrics.getRegressionMetrics != null)
    assert(dp.getColumns.size() == 608)
  }

  test("test WhyLogsSession with RankingMetrics") {
    import com.whylogs.spark.WhyLogs._

    val file = Files.createTempFile("data", ".parquet")
    Files.copy(WhyLogs.getClass.getResourceAsStream("/prediction_data.parquet"), file, StandardCopyOption.REPLACE_EXISTING)
    val predictionAndLabelsRDD = spark.sparkContext.parallelize(
      Seq(
        (Array(1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Array(1, 2, 3, 4, 5)),
        (Array(4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array(1, 2, 3)),
        (Array(1, 2, 3, 4, 5), Array(0, 0, 0, 0, 0))),
      2)
    val predictionAndLabelsDF = spark.createDataFrame(predictionAndLabelsRDD).toDF("predictions", "labels")
    println(s"predictionAndLabelsDF: ${predictionAndLabelsDF}")
    println(s"predictionAndLabelsDF schema: ${predictionAndLabelsDF.printSchema()}")
    val selectedDF = predictionAndLabelsDF.select("predictions", "labels")
    val rddOfTuples = selectedDF.rdd
      .map(row => (
        row.getAs[Seq[Int]]("predictions").toArray,
        row.getAs[Seq[Int]]("labels").toArray
    ))

    val eps = 1.0e-5
    val metrics = new RankingMetrics(rddOfTuples)
    val map = metrics.meanAveragePrecision

    assert(metrics.precisionAt(1) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(2) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(3) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(4) ~== 0.75 / 3 absTol eps)
    assert(metrics.precisionAt(5) ~== 0.8 / 3 absTol eps)
    assert(metrics.precisionAt(10) ~== 0.8 / 3 absTol eps)
    assert(metrics.precisionAt(15) ~== 8.0 / 45 absTol eps)

    println(s"  *** precision@1=${metrics.precisionAt(1)}")

    assert(map ~== 0.355026 absTol eps)

    assert(metrics.meanAveragePrecisionAt(1) ~== 0.333334 absTol eps)
    assert(metrics.meanAveragePrecisionAt(2) ~== 0.25 absTol eps)
    assert(metrics.meanAveragePrecisionAt(3) ~== 0.24074 absTol eps)
    println(s"  *** map@1=${metrics.meanAveragePrecisionAt(1)}")

    assert(metrics.ndcgAt(3) ~== 1.0 / 3 absTol eps)
    assert(metrics.ndcgAt(5) ~== 0.328788 absTol eps)
    assert(metrics.ndcgAt(10) ~== 0.487913 absTol eps)
    assert(metrics.ndcgAt(15) ~== metrics.ndcgAt(10) absTol eps)
    println(s"  *** ndcg@3=${metrics.ndcgAt(3)}")

    assert(metrics.recallAt(1) ~== 1.0 / 15 absTol eps)
    assert(metrics.recallAt(2) ~== 8.0 / 45 absTol eps)
    assert(metrics.recallAt(3) ~== 11.0 / 45 absTol eps)
    assert(metrics.recallAt(4) ~== 11.0 / 45 absTol eps)
    assert(metrics.recallAt(5) ~== 16.0 / 45 absTol eps)
    assert(metrics.recallAt(10) ~== 2.0 / 3 absTol eps)
    assert(metrics.recallAt(15) ~== 2.0 / 3 absTol eps)
    println(s"  *** recall@3=${metrics.recallAt(1)}")

    val testK = 2
    val metricsSequence = Seq((
      metrics.precisionAt(testK),
      metrics.meanAveragePrecisionAt(testK),
      metrics.ndcgAt(testK),
      metrics.recallAt(testK)))
    println(metricsSequence)

    val metricsFunctions = new RankingMetricsUDF[Int]("predictions", "labels", k=testK)

    val output_df = metricsFunctions.applyMetrics(predictionAndLabelsDF)
    val precisionColumnName = s"precision_k_$testK"
    val meanAveragePrecisionColumnName = s"average_precision_k_$testK"
    val ndcgColumnName = s"norm_dis_cumul_gain_k_$testK"
    val recallAtColumnName = s"recall_k_$testK"
    assert(output_df.columns.contains(precisionColumnName))
    assert(output_df.columns.contains(meanAveragePrecisionColumnName))
    assert(output_df.columns.contains(ndcgColumnName))
    assert(output_df.columns.contains(recallAtColumnName))

    val computed_precision_k_2 = output_df.agg(mean(col(precisionColumnName)).alias("ranking_metric_mean")).first().getAs[Double]("ranking_metric_mean")
    val computed_meanAveragePrecision_k_2 = output_df.agg(mean(col(meanAveragePrecisionColumnName)).alias("ranking_metric_mean")).first().getAs[Double]("ranking_metric_mean")
    val computed_ndcg_k_2 = output_df.agg(mean(col(ndcgColumnName)).alias("ranking_metric_mean")).first().getAs[Double]("ranking_metric_mean")
    val computed_recall_k_2 = output_df.agg(mean(col(recallAtColumnName)).alias("ranking_metric_mean")).first().getAs[Double]("ranking_metric_mean")

    assert(metrics.precisionAt(testK) ~== computed_precision_k_2 absTol eps)
    assert(metrics.meanAveragePrecisionAt(testK) ~== computed_meanAveragePrecision_k_2 absTol eps)
    assert(metrics.ndcgAt(testK) ~== computed_ndcg_k_2 absTol eps)
    assert(metrics.recallAt(testK) ~== computed_recall_k_2 absTol eps)
    println(output_df.show())
  }

  test("test RankingMetrics withTimecolumn") {
    import com.whylogs.spark.WhyLogs._
    import java.time._

    val today = Timestamp.valueOf(LocalDateTime.now())
    val yesterday = Timestamp.valueOf(LocalDateTime.now().minusDays(1))
    val input_df = spark.createDataFrame(Seq(
      ("group1", today, Array(1, 6, 2), Array(1, 2, 4), Array(0.9, 0.8, 0.7)),
      ("group1", today, Array(4, 1, 5), Array(1, 3, 4), Array(0.1, 0.4, 0.5)),
      ("group2", today, Array(4, 1, 5), Array(1, 3, 4), Array(0.1, 0.4, 0.5)),
      ("group1", yesterday, Array(4, 1, 5), Array(1, 3, 4), Array(0.1, 0.4, 0.5)),
      ("group1", yesterday, Array(4, 1, 5), Array(1, 3, 4), Array(0.1, 0.4, 0.5)),
      ("group2", yesterday, Array(1, 2, 3), Array(3, 2, 4), Array(0.2, 0.5, 0.3))
    )).toDF("group", "dataset_timestamp", "predictions", "targets", "scores")

     val res = input_df.newProfilingSession("ranking")
      .withTimeColumn("dataset_timestamp")
      .withRankingMetrics("predictions", "targets", "scores")
      .groupBy("group")
      .aggProfiles()
    val profile_count = res.count()
    assert(profile_count == 4, s"We expect profiles for 2 dates each with two segments, got a total profile count of $profile_count")
  }

  test("profile null value") {
    val schema = List(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = true)
    )

    val data = Seq(
      Row("miguel", null),
      Row("luisa", 21)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val res = df.newProfilingSession("model")
      .aggProfiles(Instant.now())
    res.count()
    val bytes = res.collect()(0).getAs[Array[Byte]](0)
    val dp = DatasetProfile.parse(new ByteArrayInputStream(bytes))

    assert(dp.getColumns.get("age").getSchemaTracker.getTypeCounts.get(InferredType.Type.NULL) == 1)
  }
}
