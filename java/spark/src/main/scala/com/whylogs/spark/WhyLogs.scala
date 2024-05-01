package com.whylogs.spark

import ai.whylabs.service.api.LogApi
import ai.whylabs.service.invoker.ApiClient
import ai.whylabs.service.model.{LogAsyncRequest, SegmentTag}
import com.whylogs.spark.WhyLogs.PROFILE_FIELD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, IntegerType, NumericType, StringType, StructField}

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row}
import org.apache.spark.whylogs.{DatasetProfileAggregator, DatasetProfileMerger}
import org.slf4j.LoggerFactory

import java.io.ByteArrayInputStream
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class ModelProfileSession(predictionField: String, targetField: String, scoreField: String = null) {
  def shouldExclude(field: String): Boolean = {
    predictionField == field || targetField == field || scoreField == field
  }
}
case class RankingMetricsSession[T : TypeTag : ClassTag](predictionField: String, targetField: String, scoreField: String = null, k: Int = 10) {
  val udfs = new RankingMetricsUDF[T](predictionField, targetField, scoreField, k)
}

/**
 * A class that enable easy access to the profiling API
 *
 * @param dataFrame      the dataframe to profile
 * @param name           the name of the dataset
 * @param timeColumn     the time column, if the data is to be broken down by time
 * @param groupByColumns the group by column
 */
case class WhyProfileSession(private val dataFrame: DataFrame,
                             private val name: String,
                             private val timeColumn: String = null,
                             private val groupByColumns: Seq[String] = List(),
                             // model metrics
                             private val modelProfile: ModelProfileSession = null,
                             private val rankingMetrics: RankingMetricsSession[_] = null
                            ) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val columnNames = dataFrame.schema.fieldNames.toSet

  /**
   * Set the column for grouping by time. This column must be of Timestamp type in Spark SQL.
   *
   * Note that WhyLogs uses this column to group data together, so please make sure you truncate the
   * data to the appropriate level of precision (i.e. daily, hourly) before calling this.
   * We only accept a column name at the moment. You can alias raw Column into a column name with
   * [[Dataset#withColumn(colName: String, col: Column)]]
   *
   * @param timeColumn the column that contains the timestamp.
   * @return
   */
  def withTimeColumn(timeColumn: String): WhyProfileSession = {
    checkIfColumnExists(timeColumn)

    val idx = dataFrame.schema.fieldIndex(timeColumn)
    val field = dataFrame.schema.fields(idx)
    if (!field.dataType.equals(DataTypes.TimestampType)) {
      throw new IllegalArgumentException(s"Unsupported timestamp column: ${field.name} - type: ${field.dataType}")
    }
    this.copy(timeColumn = timeColumn)
  }

  def groupBy(col1: String, cols: String*): WhyProfileSession = {
    (cols :+ col1).foreach(checkIfColumnExists)

    this.copy(groupByColumns = cols :+ col1)
  }

  /**
   * A Java friendly API. This is used by the Py4J gateway to pass data
   * into the JV
   *
   * @param columns list of columns for grouping
   * @return a new WhyProfileSession object
   */
  def groupBy(columns: java.util.List[String]): WhyProfileSession = {
    this.copy(groupByColumns = columns.asScala)
  }

  def withClassificationModel(predictionField: String, targetField: String, scoreField: String): WhyProfileSession = {
    checkIfColumnExists(predictionField)
    checkIfColumnExists(targetField)

    this.copy(modelProfile = ModelProfileSession(predictionField, targetField, scoreField))
  }

  def withRegressionModel(predictionField: String, targetField: String): WhyProfileSession = {
    checkIfColumnExists(predictionField)
    checkIfColumnExists(targetField)

    val predFieldSchema: StructField = dataFrame.schema.apply(predictionField)
    if (!predFieldSchema.dataType.isInstanceOf[NumericType]) {
      throw new IllegalStateException(s"Prediction field MUST be of numeric type. Got: ${predFieldSchema.dataType}")
    }
    val targetFieldSchema: StructField = dataFrame.schema.apply(targetField)
    if (!predFieldSchema.dataType.isInstanceOf[NumericType]) {
      throw new IllegalStateException(s"Target field MUST be of numeric type. Got: ${targetFieldSchema.dataType}")
    }

    this.copy(modelProfile = ModelProfileSession(predictionField, targetField))
  }

  def withRankingMetrics(predictionField: String, targetField: String, scoreField: String = null, k: Int = 10) : WhyProfileSession = {
    checkIfColumnExists(predictionField)
    checkIfColumnExists(targetField)

    val predFieldSchema: StructField = dataFrame.schema.apply(predictionField)
    if (!predFieldSchema.dataType.isInstanceOf[ArrayType]) {
      throw new IllegalStateException(s"Ranking Metrics prediction field MUST be of array of numeric type. Got: ${predFieldSchema.dataType}")
    }
    val targetFieldSchema: StructField = dataFrame.schema.apply(targetField)
    if (!targetFieldSchema.dataType.isInstanceOf[ArrayType]) {
      throw new IllegalStateException(s"Ranking Metrics target field MUST be of array numeric type. Got: ${targetFieldSchema.dataType}")
    }

    // check data type is supported for ranking metrics
    def isValidMetricType(dataType: DataType): Boolean = dataType match {
      case ArrayType(elementType, _) => elementType match {
        case _: IntegerType | _: StringType => true
        case _ => false
      }
      case _ => false
    }

    // Validate types of prediction and target field array elements
    if (!isValidMetricType(predFieldSchema.dataType) || !isValidMetricType(targetFieldSchema.dataType) || predFieldSchema.dataType != targetFieldSchema.dataType) {
      throw new IllegalStateException(s"Ranking Metrics fields MUST be arrays of either integer or string type. Found: ${predFieldSchema.dataType} and ${targetFieldSchema.dataType}")
    }

    (predFieldSchema.dataType, targetFieldSchema.dataType) match {
      case (ArrayType(_: IntegerType, _), ArrayType(_: IntegerType, _)) =>
        this.copy(rankingMetrics = RankingMetricsSession[Int](predictionField, targetField, scoreField, k))
      case (ArrayType(_: StringType, _), ArrayType(_: StringType, _)) =>
        this.copy(rankingMetrics = RankingMetricsSession[String](predictionField, targetField, scoreField, k))
      case _ => throw new IllegalArgumentException(s"Unsupported combination of data types for RankingMetricsUDF: ( ${predFieldSchema.dataType} and ${targetFieldSchema.dataType})")
    }
  }

  def aggRankingMetricsProfiles(timestamp: Long): DataFrame = {
    val debugGroupByStr = groupByColumns.mkString(",")
    logger.debug(s"Ranking metrics session name: $name")
    logger.debug(s"timestamp: $timestamp")
    logger.debug(s"Time column: $timeColumn")
    logger.debug(s"Group by columns: $debugGroupByStr")
    logger.debug(s"All columns: $columnNames")

    val dataFrameWithRankingMetrics = rankingMetrics.udfs.applyMetrics(dataFrame)
    this.aggProfiles(Instant.ofEpochMilli(timestamp), dataFrameWithRankingMetrics)
  }

  /**
   * Run aggregation and build profile based on the specification of this session
   *
   * @param timestamp the session timestamp for the whole run
   * @return a DataFrame with aggregated profiles under 'why_profile' column
   */
  def aggProfiles(timestamp: Long): DataFrame = {
    this.aggProfiles(Instant.ofEpochMilli(timestamp), this.dataFrame)
  }

  /**
   * Run aggregation and build profile based on the specification of this session
   *
   * @param timestamp the session timestamp for the whole run (often the current time, or the start of the
   *                  batch run
   * @return a DataFrame with aggregated profiles under 'why_profile' column
   */
  def aggProfiles(timestamp: Instant = Instant.now()): DataFrame = {
    val debugGroupByStr = groupByColumns.mkString(",")
    logger.debug(s"Session name: $name")
    logger.debug(s"Time column: $timeColumn")
    logger.debug(s"Group by columns: $debugGroupByStr")
    logger.debug(s"All columns: $columnNames")
    this.aggProfiles(timestamp, this.dataFrame)
  }

  def aggProfiles(timestamp: Instant, inputDataFrame: DataFrame): DataFrame = {
    val timeInMillis = timestamp.toEpochMilli

    // very important: we don't want the job to have a huge number of partitions
    // it's counter intuitive but the jobs are CPU bound and thus are better to be bounded
    // by the default parallelism value
    val coalesced = inputDataFrame.coalesce(inputDataFrame.sparkSession.sparkContext.defaultParallelism)
    val debugGroupByStr = groupByColumns.mkString(",")
    logger.info(s"Run profiling with: [$name, $timestamp] with time column [$timeColumn], group by: $debugGroupByStr")
    val groupByWithTime = groupByColumns ++ Option(timeColumn).toSeq

    val profileMetricsFields = Option(modelProfile).toSeq.flatMap(m => {
      Seq(m.targetField, m.predictionField, m.scoreField).filter(_ != null)
    })

    val fields = inputDataFrame.schema.fields.map(_.name)
    val remainingFields = fields.filter(!groupByWithTime.contains(_)).filter(!profileMetricsFields.contains(_))
    val columnGroups = remainingFields.grouped(100).toSeq

    val primaryProfiles = coalesced.select((groupByWithTime ++ profileMetricsFields ++ columnGroups.head).map(col):_*)
      .groupBy(groupByWithTime.map(col):_*)
      .agg(DatasetProfileAggregator(name, timeInMillis, timeColumn, groupByColumns, modelProfile)
        .toColumn
        .alias(PROFILE_FIELD))

    (
      Seq(primaryProfiles) ++
      // adding the rest of the columns
      columnGroups
      .tail
      .map(cols => {
        val targetFields = groupByWithTime ++ cols
        val filteredDf = coalesced.select(targetFields.head, targetFields.tail: _*)

        val res = filteredDf.groupBy(groupByWithTime.map(col):_*)
          .agg(DatasetProfileAggregator(name, timeInMillis, timeColumn, groupByColumns)
            .toColumn
            .alias(PROFILE_FIELD))
        res
      })
     )
      .reduce((left, right) => {
        left.union(right)
      })
      .groupBy(groupByWithTime.map(col):_*)
      .agg(new DatasetProfileMerger(name, timeInMillis).toColumn.alias(PROFILE_FIELD))
  }

  def log(timestampInMs: Long = Instant.now().toEpochMilli,
          orgId: String,
          modelId: String,
          apiKey: String,
          endpoint: String = "https://api.whylabsapp.com",
          sslCaCertData: String = null,
         ): Unit = {
    val df = aggProfiles(timestamp = timestampInMs)

    df.foreachPartition((rows: Iterator[Row]) => {
      doUpload(orgId, modelId, apiKey, rows, endpoint, sslCaCertData)
    })
  }

  def logRankingMetrics(timestampInMs: Long = Instant.now().toEpochMilli,
          orgId: String,
          modelId: String,
          apiKey: String,
          endpoint: String = "https://api.whylabsapp.com",
          sslCaCertData: String = null,
         ): Unit = {

    val df = aggRankingMetricsProfiles(timestampInMs)
    df.foreachPartition((rows: Iterator[Row]) => {
      doUpload(orgId, modelId, apiKey, rows, endpoint, sslCaCertData, Instant.ofEpochMilli(timestampInMs))
    })
  }

  private def doUpload(
                        orgId: String,
                        modelId: String,
                        apiKey: String,
                        rows: Iterator[Row],
                        endpoint: String,
                        sslCaCertData: String = null,
                        timestamp: Instant = null
                      ): Unit = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(endpoint)
    client.setApiKey(apiKey)
    Option(sslCaCertData).foreach(it => {
      val is = new ByteArrayInputStream(it.getBytes(StandardCharsets.UTF_8))
      client.setSslCaCert(is)
    })

    val logApi = new LogApi(client)

    rows.foreach(row => {
      uploadRow(logApi, orgId, modelId, row, timestamp)
    })
  }


  private def uploadRow(logApi: LogApi, orgId: String, modelId: String, row: Row, session_timestamp: Instant = null): Unit = {
    import RowHelper._

    val timestamp: Long = if (timeColumn != null) {
      row.getTimestampInMs(timeColumn)
    } else if (session_timestamp != null) {
      session_timestamp.toEpochMilli
    } else {
      Instant.now().toEpochMilli
    }

    val segmentTags = groupByColumns
      .toSet
      .map((f: String) => f -> Option(row.getAsText(f)))
      .filter(_._2.nonEmpty)
      .map(e => e._1 -> e._2.get)
      .map(e => new SegmentTag().key(e._1).value(e._2))
      .toList
      .asJava

    val profileData = row.getByteArray(PROFILE_FIELD)

    val tmp = Files.createTempFile("profile", ".bin")

    try {
      Files.write(tmp, profileData, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

      // Create the upload url
      val uploadResultFuture = RetryUtil.withRetries() {
        val req = new LogAsyncRequest()
        req.setSegmentTags(segmentTags)
        req.datasetTimestamp(timestamp)
        logApi.logAsync(orgId, modelId, req)
      }
      val uploadResult = Await.result(uploadResultFuture, Duration.create(10, TimeUnit.SECONDS))

      // Write the profile to the upload url
      val profileUploadResult = RetryUtil.withRetries() {
        val connection = new URL(uploadResult.getUploadUrl)
          .openConnection()
          .asInstanceOf[HttpURLConnection]
        connection.setDoOutput(true)
        connection.setRequestProperty("Content-Type", "application/octet-stream")
        connection.setRequestMethod("PUT")

        val out = connection.getOutputStream
        try {
          Files.copy(tmp.toAbsolutePath, out)
        } finally {
          out.close()
        }

        if (connection.getResponseCode != 200) {
          throw new RuntimeException(s"Error uploading profile: ${connection.getResponseCode} ${connection.getResponseMessage}")
        }
      }

      // TODO, what is the right timeout for this? For extremely large models there could be hundreds of megabytes.
      Await.result(profileUploadResult, Duration.Inf)
    } finally {
      Files.delete(tmp)
    }
  }

  private def checkIfColumnExists(col: String): Unit = {
    if (!columnNames.contains(col)) {
      throw new IllegalArgumentException(s"Column $col does not exist. Available columns: $columnNames")
    }
  }
}

object RowHelper {

  implicit class BetterRow(row: Row) {
    private val schema = row.schema

    def getByteArray(fieldName: String): Array[Byte] = {
      row.getAs[Array[Byte]](schema.fieldIndex(fieldName))
    }

    def getTimestampInMs(fieldName: String): Long = {
      row.getTimestamp(schema.fieldIndex(fieldName)).getTime
    }

    def getAsText(fieldName: String): String = {
      val value = row.get(schema.fieldIndex(fieldName))
      if (value == null) {
        null
      } else {
        value.toString
      }
    }
  }

}

/**
 * Helper object that helps create new profiling sessions
 */
object WhyLogs {
  // we use an intermediate name so we can extract the "value" after
  val PROFILE_FIELD = "why_profile"

  implicit class ProfiledDataFrame(dataframe: Dataset[Row]) {

    def newProfilingSession(name: String): WhyProfileSession = {
      WhyProfileSession(dataframe, name)
    }

    def newProfilingSession(name: String, timeColumn: String): WhyProfileSession = {
      WhyProfileSession(dataframe, name, timeColumn)
    }
  }

  def newProfilingSession(dataframe: Dataset[Row], name: String): WhyProfileSession = {
    WhyProfileSession(dataframe, name)
  }

  def newProfilingSession(dataframe: Dataset[Row], name: String, timeColumn: String): WhyProfileSession = {
    WhyProfileSession(dataframe, name, timeColumn)
  }
}
