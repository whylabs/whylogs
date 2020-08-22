package org.apache.spark.whylabs.logs


import java.io.ByteArrayOutputStream

import com.whylogs.core.DatasetProfile
import com.whylogs.core.message.DatasetProfileMessage
import com.whylogs.core.utils.ProtobufHelper
import org.apache.spark.sql.types.{BinaryType, DataType, SQLUserDefinedType, UserDefinedType}

/**
 * Dataset API requires a case class for automatic encoders. I couldn't figure out
 * how to manually create an encoder for a Java class and thus a wrapper
 * @param value the actual DatasetProfile object
 */
@SerialVersionUID(value = -687991492884005033L)
@SQLUserDefinedType(udt = classOf[ScalaDatasetProfileUDT])
case class ScalaDatasetProfile(value: DatasetProfile) extends Serializable {
  override def toString: String = ProtobufHelper.summaryToString(value.toSummary)
}

object ScalaDatasetProfileUDT {
  private val instance = new ScalaDatasetProfileUDT()

  def apply(): ScalaDatasetProfileUDT = instance
}

/**
 * The user defined type for the ScalaDatasetProfile class.
 * This is a developer API so it might break in the future.
 */
class ScalaDatasetProfileUDT extends UserDefinedType[ScalaDatasetProfile] {
  override def typeName: String = "ScalaDatasetProfile"

  override def sqlType: DataType = BinaryType

  override def pyUDT: String = "org.apache.spark.whylabs.logs.DatasetProfileDataType"

  override def serialize(obj: ScalaDatasetProfile): Array[Byte] = {
    val msg = obj.value.toProtobuf.build()
    val bos = new ByteArrayOutputStream(msg.getSerializedSize)
    msg.writeTo(bos)
    bos.toByteArray
  }

  override def deserialize(datum: Any): ScalaDatasetProfile = {
    datum match {
      case values: Array[Byte] =>
        val profile = DatasetProfile.fromProtobuf(DatasetProfileMessage.parseFrom(values))
        ScalaDatasetProfile(profile)
    }
  }

  override def userClass: Class[ScalaDatasetProfile] = classOf[ScalaDatasetProfile]

  private[spark] override def asNullable: ScalaDatasetProfileUDT = this
}
