package org.apache.spark.whylogs

import com.whylogs.core.DatasetProfile
import com.whylogs.spark.WhyLogs.PROFILE_FIELD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

import java.io.ByteArrayInputStream
import java.time.Instant
import java.util.UUID

class DatasetProfileMerger(datasetName: String,
                           sessionTimeInMillis: Long,
                           sessionId: String = UUID.randomUUID().toString) extends Aggregator[Row, DatasetProfile, Array[Byte]] {
  override def zero: DatasetProfile =  new DatasetProfile(sessionId, Instant.ofEpochMilli(sessionTimeInMillis))
    .withTag("Name", datasetName)

  override def reduce(b: DatasetProfile, row: Row): DatasetProfile = {
    val bytes = row.getAs[Array[Byte]](PROFILE_FIELD)
    if (bytes == null || bytes.length == 0) {
      return b
    }
    val is = new ByteArrayInputStream(bytes);
    val rhs = DatasetProfile.parse(is)
    b.merge(rhs)
  }

  override def merge(b1: DatasetProfile, b2: DatasetProfile): DatasetProfile =  {
    b1.merge(b2)
  }

  override def finish(reduction: DatasetProfile): Array[Byte] = reduction.toBytes

  override def bufferEncoder: Encoder[DatasetProfile] = Encoders.javaSerialization(classOf[DatasetProfile])

  override def outputEncoder: Encoder[Array[Byte]] = ExpressionEncoder[Array[Byte]]()
}
