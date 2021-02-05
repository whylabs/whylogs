package com.whylogs.spark

import java.time.Instant

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.whylogs.DatasetProfileAggregator
import org.slf4j.LoggerFactory

import scala.language.implicitConversions

case class WhyProfileSession(private val dataFrame: DataFrame,
                             private val name: String,
                             private val timeColumn: String = null,
                             private val groupByColumns: Seq[String] = List()) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val columnNames = dataFrame.schema.fieldNames.toSet

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
   * Run aggregation and build profile based on the specification of this session
   *
   * @param timestamp the timestamp for the whole run (often the current time, or the start of the
   *                  batch run
   * @return a DataFrame with aggregated profiles under 'why_profile' column
   */
  def aggProfiles(timestamp: Instant = Instant.now()): DataFrame = {
    val debugGroupByStr = groupByColumns.mkString(",")
    logger.debug(s"Session name: $name")
    logger.debug(s"Time column: $timeColumn")
    logger.debug(s"Group by columns: $debugGroupByStr")
    logger.debug(s"All columns: $columnNames")

    // we use an intermediate name so we can extract the "value" after
    val whyStruct = "why_profile"

    val timeInMillis = timestamp.toEpochMilli
    val whyStructDataFrame =
      if (timeColumn != null) { // if timeColumn is specified
        logger.info(s"Run profiling with: [$name, $timestamp] with time column [$timeColumn], group by: $debugGroupByStr")
        val profileAgg = DatasetProfileAggregator(name, timeInMillis, timeColumn, groupByColumns)
          .toColumn
          .alias(whyStruct)
        dataFrame.groupBy(timeColumn, groupByColumns: _*)
          .agg(profileAgg)
      } else {
        logger.info(s"Run profiling with: [$name, $timestamp] without time column, group by: $debugGroupByStr")
        val profileAgg = DatasetProfileAggregator(name, timeInMillis, groupByColumns = groupByColumns)
          .toColumn
          .alias(whyStruct)
        dataFrame.groupBy(groupByColumns.map(dataFrame.col): _*)
          .agg(profileAgg)
      }

    whyStructDataFrame
  }

  private def checkIfColumnExists(col: String): Unit = {
    if (!columnNames.contains(col)) {
      throw new IllegalArgumentException(s"Column $col does not exist. Available columns: $columnNames")
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

}
