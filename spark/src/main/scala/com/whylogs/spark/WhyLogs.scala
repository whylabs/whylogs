package com.whylogs.spark

import ai.whylabs.service.api.LogApi
import ai.whylabs.service.invoker.ApiClient
import ai.whylabs.service.model.SegmentTag
import org.apache.spark.sql.types.{DataTypes, NumericType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.whylogs.DatasetProfileAggregator
import org.slf4j.LoggerFactory

import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, StandardOpenOption}
import java.time.Instant
import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class ModelProfileSession(predictionField: String, targetField: String, scoreField: String = null) {
  def shouldExclude(field: String): Boolean = {
    predictionField == field || targetField == field || scoreField == field
  }
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
                             private val modelProfile: ModelProfileSession = null
                            ) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val columnNames = dataFrame.schema.fieldNames.toSet
  // we use an intermediate name so we can extract the "value" after
  val PROFILE_FIELD = "why_profile"

  /**
   * Set the column for grouping by time. This column must be of Timestamp type in Spark SQL.
   *
   * Note that WhyLogs uses this column to group data together, so please make sure you truncate the
   * data to the appropriate level of precision (i.e. daily, hourly) before calling this.
   * We only accept a column name at the moment. You can alias raw Column into a column name with
   * [[Dataset#withColumn(colucolName: String, col: Column)]]
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

  def withModelProfile(predictionField: String, targetField: String, scoreField: String): WhyProfileSession = {
    checkIfColumnExists(predictionField)
    checkIfColumnExists(targetField)

    this.copy(modelProfile = ModelProfileSession(predictionField, targetField, scoreField))
  }

  def withModelProfile(predictionField: String, targetField: String): WhyProfileSession = {
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

  /**
   * Run aggregation and build profile based on the specification of this session
   *
   * @param timestamp the session timestamp for the whole run
   * @return a DataFrame with aggregated profiles under 'why_profile' column
   */
  def aggProfiles(timestamp: Long): DataFrame = {
    this.aggProfiles(Instant.ofEpochMilli(timestamp))
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

    val timeInMillis = timestamp.toEpochMilli
    val whyStructDataFrame =
      if (timeColumn != null) { // if timeColumn is specified
        logger.info(s"Run profiling with: [$name, $timestamp] with time column [$timeColumn], group by: $debugGroupByStr")
        val profileAgg = DatasetProfileAggregator(name, timeInMillis, timeColumn, groupByColumns, modelProfile)
          .toColumn
          .alias(PROFILE_FIELD)
        val df = dataFrame.groupBy(timeColumn, groupByColumns: _*)
          .agg(profileAgg)
        df
      } else {
        logger.info(s"Run profiling with: [$name, $timestamp] without time column, group by: $debugGroupByStr")
        val profileAgg = DatasetProfileAggregator(name, timeInMillis, groupByColumns = groupByColumns,
          model = modelProfile)
          .toColumn
          .alias(PROFILE_FIELD)
        dataFrame.groupBy(groupByColumns.map(dataFrame.col): _*)
          .agg(profileAgg)
      }

    whyStructDataFrame
  }

  def log(timestampInMs: Long = Instant.now().toEpochMilli, orgId: String, modelId: String, apiKey: String, endpoint: String = "https://api.whylabsapp.com"): Unit = {
    val df = aggProfiles(timestamp = timestampInMs)

    df.foreachPartition((rows: Iterator[Row]) => {
      doUpload(orgId, modelId, apiKey, rows, endpoint)
    })
  }

  private def doUpload(orgId: String, modelId: String, apiKey: String, rows: Iterator[Row], endpoint: String): Unit = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(endpoint)
    client.setApiKey(apiKey)

    val logApi = new LogApi(client)

    rows.foreach(row => {
      uploadRow(logApi, orgId, modelId, row)
    })
  }


  private def uploadRow(logApi: LogApi, orgId: String, modelId: String, row: Row) = {
    import RowHelper._

    val timestamp: Long = if (timeColumn != null) {
      row.getTimestampInMs(timeColumn)
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
      val uploadResult = logApi.logAsync(orgId, modelId, timestamp, segmentTags, null)

      // Write the profile to the upload url
      val connection = new URL(uploadResult.getUploadUrl)
        .openConnection()
        .asInstanceOf[HttpURLConnection]
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/octet-stream")
      connection.setRequestMethod("PUT")

      val out = connection.getOutputStream
      try {
        Files.copy(tmp.getFileName, out)
      } finally {
        out.close()
      }

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
