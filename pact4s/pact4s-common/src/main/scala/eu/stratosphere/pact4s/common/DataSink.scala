/**
 * *********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * ********************************************************************************************************************
 */

package eu.stratosphere.pact4s.common

import java.io.DataOutput
import java.io.OutputStream

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.generic.io._
import eu.stratosphere.nephele.configuration.Configuration

class DataSink[In: UDT](val url: String, val format: DataSinkFormat[In]) extends Hintable[In] with Serializable

abstract class DataSinkFormat[In: UDT] extends Serializable {

  val stub: Class[_ <: OutputFormat[_]]
  val inputUDT: UDT[In]
  val fieldSelector: FieldSelector

  def persistConfiguration(config: Configuration) = {}
}

case class RawDataSinkFormat[In: UDT, F >: (In, OutputStream) => Unit <% UDF2](val writeFunction: (In, OutputStream) => Unit) extends DataSinkFormat[In] {

  override val stub = classOf[RawOutput4sStub[In]]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = AnalyzedFieldSelector.fromIndexMap(implicitly[UDT[In]], writeFunction.getReadFields._1)

  override def persistConfiguration(config: Configuration) {

    val deserializer = inputUDT.getSerializer(AnalyzedFieldSelector.toIndexMap(fieldSelector))

    val stubParameters = RawOutputParameters(deserializer, writeFunction)
    stubParameters.persist(config)
  }
}

case class BinaryDataSinkFormat[In: UDT, F >: (In, DataOutput) => Unit <% UDF2](val writeFunction: (In, DataOutput) => Unit, val blockSize: Option[Long] = None) extends DataSinkFormat[In] {

  def this(writeFunction: (In, DataOutput) => Unit, blockSize: Long) = this(writeFunction, Some(blockSize))

  override val stub = classOf[BinaryOutput4sStub[In]]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = AnalyzedFieldSelector.fromIndexMap(implicitly[UDT[In]], writeFunction.getReadFields._1)

  override def persistConfiguration(config: Configuration) {

    val deserializer = inputUDT.getSerializer(AnalyzedFieldSelector.toIndexMap(fieldSelector))

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
  override val fieldSelector = AnalyzedFieldSelector(implicitly[UDT[In]])

  override def persistConfiguration(config: Configuration) {
    if (blockSize.isDefined)
      config.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize.get)
  }
}

case class DelimetedDataSinkFormat[In: UDT](val writeFunction: UDF2Code[(In, Array[Byte]) => Int], val delimeter: Option[String] = None) extends DataSinkFormat[In] {

  override val stub = classOf[DelimetedOutput4sStub[In]]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = AnalyzedFieldSelector.fromIndexMap(implicitly[UDT[In]], writeFunction.getReadFields._1)

  override def persistConfiguration(config: Configuration) {

    val deserializer = inputUDT.getSerializer(AnalyzedFieldSelector.toIndexMap(fieldSelector))

    val stubParameters = DelimetedOutputParameters(deserializer, writeFunction.userFunction)
    stubParameters.persist(config)

    if (delimeter.isDefined)
      config.setString(DelimitedOutputFormat.RECORD_DELIMITER, delimeter.get)
  }
}

object DelimetedDataSinkFormat {

  def apply[In: UDT](formatFunction: UDF1Code[In => String]): DelimetedDataSinkFormat[In] = forString(formatFunction, null)
  def apply[In: UDT](formatFunction: UDF1Code[In => String], delimeter: String): DelimetedDataSinkFormat[In] = forString(formatFunction, delimeter)

  private def forString[In: UDT](formatFunction: UDF1Code[In => String], delimeter: String): DelimetedDataSinkFormat[In] = {

    val fun = formatFunction.userFunction
    val reads = formatFunction.getReadFields
    val numFields = implicitly[UDT[In]].numFields

    val userFun = (source: In, target: Array[Byte]) => {
      val str = fun(source)
      val data = str.getBytes
      if (data.length <= target.length) {
        System.arraycopy(data, 0, target, 0, data.length);
        data.length;
      } else {
        -data.length;
      }
    }

    val writeFunction = new UDF2Code[(In, Array[Byte]) => Int] with AnalyzedUDF2 {
      override def userFunction = userFun
      override val leftReadFields = getInitialReadFields(numFields)
      override val rightReadFields = getInitialReadFields(0)
      override val writeFields = getInitialWriteFields(0)
    }

    for (i <- 0 until numFields if reads(i) < 0) {
      writeFunction.markInputFieldUnread(Left(i))
    }

    new DelimetedDataSinkFormat(writeFunction, maybeDelim(delimeter))
  }

  def apply[In: UDT](formatFunction: UDF2Code[(In, StringBuilder) => Unit]): DelimetedDataSinkFormat[In] = forStringBuilder(formatFunction, null)
  def apply[In: UDT](formatFunction: UDF2Code[(In, StringBuilder) => Unit], delimeter: String): DelimetedDataSinkFormat[In] = forStringBuilder(formatFunction, delimeter)

  private def forStringBuilder[In: UDT](formatFunction: UDF2Code[(In, StringBuilder) => Unit], delimeter: String): DelimetedDataSinkFormat[In] = {

    val fun = formatFunction.userFunction
    val reads = formatFunction.getReadFields._1
    val numFields = implicitly[UDT[In]].numFields

    val stringBuilder = new StringBuilder
    val userFun = (source: In, target: Array[Byte]) => {
      stringBuilder.clear
      fun(source, stringBuilder)

      val data = stringBuilder.toString.getBytes
      if (data.length <= target.length) {
        System.arraycopy(data, 0, target, 0, data.length);
        data.length;
      } else {
        -data.length;
      }
    }

    val writeFunction = new UDF2Code[(In, Array[Byte]) => Int] with AnalyzedUDF2 {
      override def userFunction = userFun
      override val leftReadFields = getInitialReadFields(numFields)
      override val rightReadFields = getInitialReadFields(0)
      override val writeFields = getInitialWriteFields(0)
    }

    for (i <- 0 until numFields if reads(i) < 0) {
      writeFunction.markInputFieldUnread(Left(i))
    }

    new DelimetedDataSinkFormat(writeFunction, maybeDelim(delimeter))
  }

  private def maybeDelim(delim: String) = if (delim == null) None else Some(delim)
}

case class RecordDataSinkFormat[In: UDT](val recordDelimeter: Option[String] = None, val fieldDelimeter: Option[String] = None, val lenient: Option[Boolean]) extends DataSinkFormat[In] {

  def this(recordDelimeter: String, fieldDelimeter: String, lenient: Boolean) = this(Some(recordDelimeter), Some(fieldDelimeter), Some(lenient))

  override val stub = classOf[RecordOutputFormat]

  override val inputUDT = implicitly[UDT[In]]
  override val fieldSelector = AnalyzedFieldSelector(implicitly[UDT[In]])

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
