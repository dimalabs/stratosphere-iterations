package eu.stratosphere.pact4s.common.streams

import java.io.DataOutput
import java.io.OutputStream

import eu.stratosphere.pact4s.common.Hintable
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.GenericDataSink
import eu.stratosphere.nephele.configuration.Configuration

case class DataSink[In: UDT](url: String, format: DataSinkFormat[In]) extends Hintable

case class PlanOutput[In: UDT](source: DataStream[In], sink: DataSink[In]) {

  def getContract: Pact4sDataSinkContract = {

    val name = sink.getPactName getOrElse "<Unnamed File Data Sink>"
    val contract = sink.format.getContract(source.getContract, sink.url, name)
    sink.applyHints(contract)
    contract
  }
}

object PlanOutput {

  implicit def planOutput2Seq[In](p: PlanOutput[In]): Seq[PlanOutput[In]] = Seq(p)
}

abstract class DataSinkFormat[In: UDT] {

  def getContract(input: Pact4sContract, url: String, name: String): Pact4sDataSinkContract
}

case class RawDataSinkFormat[In: UDT, F: SelectorBuilder[In, Unit]#Selector](val writeFunction: (In, OutputStream) => Unit) extends DataSinkFormat[In] {

  override def getContract(input: Pact4sContract, url: String, name: String) = {

    new FileDataSink(classOf[RawOutput4sStub[In]], url, input, name) with RawDataSink4sContract[In] {

      override val stubParameters = RawOutputParameters(implicitly[UDT[In]], implicitly[FieldSelector[In => Unit]], writeFunction)
    }
  }
}

case class BinaryDataSinkFormat[In: UDT, F: SelectorBuilder[In, Unit]#Selector](val writeFunction: (In, DataOutput) => Unit, val blockSize: Option[Long] = None) extends DataSinkFormat[In] {

  def this(writeFunction: (In, DataOutput) => Unit, blockSize: Long) = this(writeFunction, Some(blockSize))

  override def getContract(input: Pact4sContract, url: String, name: String) = {

    new FileDataSink(classOf[BinaryOutput4sStub[In]], url, input, name) with BinaryDataSink4sContract[In] {

      override val stubParameters = BinaryOutputParameters(implicitly[UDT[In]], implicitly[FieldSelector[In => Unit]], writeFunction)
    }
  }
}

case class SequentialDataSinkFormat[In: UDT](val blockSize: Option[Long] = None) extends DataSinkFormat[In] {

  def this(blockSize: Long) = this(Some(blockSize))

  override def getContract(input: Pact4sContract, url: String, name: String) = {

    new FileDataSink(classOf[SequentialOutputFormat], url, input, name) with SequentialDataSink4sContract {

      if (blockSize.isDefined)
        this.getParameters().setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize.get)
    }
  }
}

case class DelimetedDataSinkFormat[In: UDT, F: SelectorBuilder[In, Unit]#Selector](val writeFunction: (In, Array[Byte]) => Int, val delimeter: Option[String] = None) extends DataSinkFormat[In] {

  def this(writeFunction: (In, Array[Byte]) => Int, delimeter: String) = this(writeFunction, Some(delimeter))

  override def getContract(input: Pact4sContract, url: String, name: String) = {

    new FileDataSink(classOf[DelimetedOutput4sStub[In]], url, input, name) with DelimetedDataSink4sContract[In] {

      override val stubParameters = DelimetedOutputParameters(implicitly[UDT[In]], implicitly[FieldSelector[In => Unit]], writeFunction)
    }
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

  override def getContract(input: Pact4sContract, url: String, name: String) = {

    new FileDataSink(classOf[RecordOutputFormat], url, input, name) with RecordDataSink4sContract {

      val fields = implicitly[UDT[In]].fieldTypes

      this.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, fields.length)

      for (fieldNum <- 0 until fields.length)
        this.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + fieldNum, fields(fieldNum))

      if (recordDelimeter.isDefined)
        this.getParameters().setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, recordDelimeter.get)

      if (fieldDelimeter.isDefined)
        this.getParameters().setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, fieldDelimeter.get)

      if (lenient.isDefined)
        this.getParameters().setBoolean(RecordOutputFormat.LENIENT_PARSING, lenient.get)
    }
  }
}
