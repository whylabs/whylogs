package org.apache.spark.whylogs

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.{Collections, UUID}

import com.whylogs.core.DatasetProfile
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object InstantDateTimeFormatter {
  private val Formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC)

  def format(instant: Instant): String = {
    Formatter.format(instant)
  }
}

/**
 * A dataset aggregator. It aggregates [[Row]] into DatasetProfile objects
 * underneath the hood.
 *
 */
case class DatasetProfileAggregator(datasetName: String,
                                    sessionTimeInMillis: Long,
                                    timeColumn: String = null,
                                    groupByColumns: Seq[String] = Seq(),
                                    sessionId: String = UUID.randomUUID().toString)
  extends Aggregator[Row, DatasetProfile, ScalaDatasetProfile] with Serializable {

  private val allGroupByColumns = (groupByColumns ++ Option(timeColumn).toSeq).toSet

  override def zero: DatasetProfile = new DatasetProfile(sessionId, Instant.ofEpochMilli(0))

  override def reduce(profile: DatasetProfile, row: Row): DatasetProfile = {
    val schema = row.schema

    val dataTimestamp = Option(timeColumn)
      .map(schema.fieldIndex)
      .map(row.getTimestamp)
      .map(_.toInstant)

    val dataTimestampString = dataTimestamp.map(InstantDateTimeFormatter.format)

    val tags = getTagsFromRow(row) ++
      dataTimestampString.map(value => (timeColumn, value)).toMap ++
      Map("Name" -> datasetName)

    val timedProfile: DatasetProfile = dataTimestamp match {
      case None if isProfileEmpty(profile) =>
        // we have an empty profile
        new DatasetProfile(sessionId,
          Instant.ofEpochMilli(sessionTimeInMillis),
          null,
          tags.asJava,
          Collections.emptyMap())
      case Some(ts) if isProfileEmpty(profile) =>
        // create a new profile to replace the empty profile
        new DatasetProfile(sessionId,
          Instant.ofEpochMilli(sessionTimeInMillis),
          ts,
          tags.asJava,
          Collections.emptyMap())
      case Some(ts) if ts != profile.getDataTimestamp =>
        throw new IllegalStateException(s"Mismatched session timestamp. " +
          s"Previously seen ts: [${profile.getDataTimestamp}]. Current session timestamp: $ts")
      case _ =>
        // ensure tags match
        if (profile.getTags != tags.asJava) {
          throw new IllegalStateException(s"Mismatched grouping columns. " +
            s"Previously seen values: ${profile.getTags}. Current values: ${tags.asJava}")
        }

        profile
    }

    // TODO: we have the schema here. Support schema?
    for (field: StructField <- schema) {
      if (!allGroupByColumns.contains(field.name)) {
        timedProfile.track(field.name, row.get(schema.fieldIndex(field.name)))
      }
    }

    timedProfile
  }

  private def isProfileEmpty(profile: DatasetProfile) = {
    profile.getDataTimestamp == null && profile.getColumns.isEmpty
  }

  private def getTagsFromRow(row: Row): Map[String, String] = {
    val schema = row.schema
    groupByColumns
      .map(col => (col, schema.fieldIndex(col)))
      .map(idxCol => (idxCol._1, Option(row.get(idxCol._2)).map(_.toString).getOrElse("")))
      .toMap
  }

  override def merge(profile1: DatasetProfile, profile2: DatasetProfile): DatasetProfile = {
    if (profile1.getColumns.isEmpty) return profile2
    if (profile2.getColumns.isEmpty) return profile1

    profile1.merge(profile2)
  }

  override def finish(reduction: DatasetProfile): ScalaDatasetProfile = {
    val finalProfile = new DatasetProfile(
      datasetName,
      reduction.getSessionTimestamp,
      reduction.getDataTimestamp,
      reduction.getTags,
      reduction.getColumns
    )
    ScalaDatasetProfile(finalProfile)
  }

  override def bufferEncoder: Encoder[DatasetProfile] = Encoders.javaSerialization(classOf[DatasetProfile])

  /**
   * To understand the detailed implementation of this class, see  [[ExpressionEncoder]].
   *
   * We use some internal Spark API here.
   */
  override def outputEncoder: Encoder[ScalaDatasetProfile] = {
    val dataType = ScalaDatasetProfileUDT()
    val structType = new StructType().add("value", dataType)

    // based on ExpressionEncoder
    // TODO: understand why we can't directly use use the 'dataType' object here
    // but have to refer to the reflection logic (it returns an ObjectType)
    val reflectionType = ScalaReflection.dataTypeFor[ScalaDatasetProfile]
    val inputObject = BoundReference(0, reflectionType, nullable = true)
    val serializer = ScalaReflection.serializerFor[ScalaDatasetProfile](inputObject)
    val deserializer = ScalaReflection.deserializerFor[ScalaDatasetProfile]

    new ExpressionEncoder[ScalaDatasetProfile](
      structType,
      flat = false,
      serializer.flatten,
      deserializer,
      ClassTag(dataType.userClass)
    )
  }
}
