package eu.stratosphere.pact4s.common

import java.io.DataOutput
import java.io.OutputStream

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.io._
import eu.stratosphere.nephele.configuration.Configuration

class DataSink[In: UDT](val url: String, val format: DataSinkFormat[In]) extends Hintable[In] with Serializable

abstract class DataSinkFormat[In: UDT] extends Serializable {

  val stub: Class[_ <: OutputFormat]
  val inputUDT: UDT[In]
  val fieldSelector: FieldSelector[In => Unit]

  def persistConfiguration(config: Configuration) = {}
}

case class RawDataSinkFormat[In: UDT, F: SelectorBuilder[In, Unit]#Selector](val writeFunction: (In, OutputStream) => Unit) extends DataSinkFormat[In] {

  override val stub = classOf[RawOutput4sStub[In]]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = implicitly[FieldSelector[In => Unit]]

  override def persistConfiguration(config: Configuration) {

    val deserializer = inputUDT.createSerializer(fieldSelector.getFields)

    val stubParameters = RawOutputParameters(deserializer, writeFunction)
    stubParameters.persist(config)
  }
}

case class BinaryDataSinkFormat[In: UDT, F: SelectorBuilder[In, Unit]#Selector](val writeFunction: (In, DataOutput) => Unit, val blockSize: Option[Long] = None) extends DataSinkFormat[In] {

  def this(writeFunction: (In, DataOutput) => Unit, blockSize: Long) = this(writeFunction, Some(blockSize))

  override val stub = classOf[BinaryOutput4sStub[In]]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = implicitly[FieldSelector[In => Unit]]

  override def persistConfiguration(config: Configuration) {

    val deserializer = inputUDT.createSerializer(fieldSelector.getFields)

    val stubParameters = BinaryOutputParameters(deserializer, writeFunction)
    stubParameters.persist(config)

    if (blockSize.isDefined)
      config.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize.get)
  }
}

case class SequentialDataSinkFormat[In: UDT](val blockSize: Option[Long] = None) extends DataSinkFormat[In] {

  def this(blockSize: Long) = this(Some(blockSize))

  override val stub = classOf[SequentialOutputFormat]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = defaultFieldSelectorT[In, Unit]

  override def persistConfiguration(config: Configuration) {
    if (blockSize.isDefined)
      config.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize.get)
  }
}

case class DelimetedDataSinkFormat[In: UDT, F: SelectorBuilder[In, Unit]#Selector](val writeFunction: (In, Array[Byte]) => Int, val delimeter: Option[String] = None) extends DataSinkFormat[In] {

  def this(writeFunction: (In, Array[Byte]) => Int, delimeter: String) = this(writeFunction, Some(delimeter))

  override val stub = classOf[DelimetedOutput4sStub[In]]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = implicitly[FieldSelector[In => Unit]]

  override def persistConfiguration(config: Configuration) {

    val deserializer = inputUDT.createSerializer(fieldSelector.getFields)

    val stubParameters = DelimetedOutputParameters(deserializer, writeFunction)
    stubParameters.persist(config)

    if (delimeter.isDefined)
      config.setString(DelimitedOutputFormat.RECORD_DELIMITER, delimeter.get)
  }
}

object DelimetedDataSinkFormat {

  def apply[In: UDT, F: SelectorBuilder[In, Unit]#Selector](formatFunction: In => String): DelimetedDataSinkFormat[In, F] = DelimetedDataSinkFormat(asWriteFunction(formatFunction) _, None)
  def apply[In: UDT, F: SelectorBuilder[In, Unit]#Selector](formatFunction: In => String, delimeter: String): DelimetedDataSinkFormat[In, F] = DelimetedDataSinkFormat(asWriteFunction(formatFunction) _, Some(delimeter))
  def apply[In: UDT, F: SelectorBuilder[In, Unit]#Selector](formatFunction: (In, StringBuilder) => Unit): DelimetedDataSinkFormat[In, F] = DelimetedDataSinkFormat(asWriteFunction(formatFunction, new StringBuilder) _, None)
  def apply[In: UDT, F: SelectorBuilder[In, Unit]#Selector](formatFunction: (In, StringBuilder) => Unit, delimeter: String): DelimetedDataSinkFormat[In, F] = DelimetedDataSinkFormat(asWriteFunction(formatFunction, new StringBuilder) _, Some(delimeter))

  private def asWriteFunction[In: UDT](formatFunction: In => String)(source: In, target: Array[Byte]): Int = {

    val str = formatFunction(source)
    writeString(str, target)
  }

  private def asWriteFunction[In: UDT](formatFunction: (In, StringBuilder) => Unit, stringBuilder: StringBuilder)(source: In, target: Array[Byte]): Int = {

    stringBuilder.clear
    formatFunction(source, stringBuilder)
    writeString(stringBuilder.toString, target)
  }

  private def writeString(source: String, target: Array[Byte]): Int = {

    val data = source.getBytes

    if (data.length <= target.length) {
      System.arraycopy(data, 0, target, 0, data.length);
      data.length;
    } else {
      -data.length;
    }
  }
}

case class RecordDataSinkFormat[In: UDT](val recordDelimeter: Option[String] = None, val fieldDelimeter: Option[String] = None, val lenient: Option[Boolean]) extends DataSinkFormat[In] {

  def this(recordDelimeter: String, fieldDelimeter: String, lenient: Boolean) = this(Some(recordDelimeter), Some(fieldDelimeter), Some(lenient))

  override val stub = classOf[RecordOutputFormat]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = defaultFieldSelectorT[In, Unit]

  override def persistConfiguration(config: Configuration) {

    val fields = inputUDT.fieldTypes

    config.setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, fields.length)

    for (fieldNum <- 0 until fields.length)
      config.setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + fieldNum, fields(fieldNum))

    if (recordDelimeter.isDefined)
      config.setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, recordDelimeter.get)

    if (fieldDelimeter.isDefined)
      config.setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, fieldDelimeter.get)

    if (lenient.isDefined)
      config.setBoolean(RecordOutputFormat.LENIENT_PARSING, lenient.get)
  }
}
