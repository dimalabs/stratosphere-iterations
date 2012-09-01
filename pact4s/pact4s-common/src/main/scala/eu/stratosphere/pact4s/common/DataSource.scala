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

import java.io.DataInput
import java.net.URI

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts.DataSource4sContract
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract.FileDataSource
import eu.stratosphere.pact.common.contract.GenericDataSource
import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.generic.io._
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base._
import eu.stratosphere.pact.common.`type`.base.parser._
import eu.stratosphere.nephele.configuration.Configuration

class DataSource[Out: UDT](url: String, format: DataSourceFormat[Out]) extends DataStream[Out] {

  override def createContract = {

    val uri = getUri(url)
    uri.getScheme match {

      case "file" | "hdfs" => new FileDataSource(format.stub.asInstanceOf[Class[FileInputFormat]], uri.toString) with DataSource4sContract[Out] {

        override val outputUDT = format.outputUDT
        override val fieldSelector = format.fieldSelector

        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
      }

      case "ext" => new GenericDataSource(format.stub.asInstanceOf[Class[InputFormat[_, _]]]) with DataSource4sContract[Out] {

        override val outputUDT = format.outputUDT
        override val fieldSelector = format.fieldSelector

        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
      }
    }
  }

  private def getUri(url: String) = {
    val uri = new URI(url)
    if (uri.getScheme == null)
      new URI("file://" + url)
    else
      uri
  }
}

abstract class DataSourceFormat[Out: UDT] extends Serializable {

  val stub: Class[_ <: InputFormat[_, _]]
  val outputUDT: UDT[Out] = implicitly[UDT[Out]]
  val fieldSelector: FieldSelector = AnalyzedFieldSelector(implicitly[UDT[Out]])

  def persistConfiguration(config: Configuration) = {}
}

case class BinaryDataSourceFormat[Out: UDT](val readFunction: DataInput => Out, val blockSize: Option[Long] = None) extends DataSourceFormat[Out] {

  def this(readFunction: DataInput => Out, blockSize: Long) = this(readFunction, Some(blockSize))

  override val stub = classOf[BinaryInput4sStub[Out]]

  override def persistConfiguration(config: Configuration) {

    val serializer = outputUDT.getSerializer(fieldSelector.getFields)

    val stubParameters = BinaryInputParameters(serializer, readFunction)
    stubParameters.persist(config)

    if (blockSize.isDefined)
      config.setLong(BinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize.get)
  }
}

case class SequentialDataSourceFormat[Out: UDT](val blockSize: Option[Long] = None) extends DataSourceFormat[Out] {

  def this(blockSize: Long) = this(Some(blockSize))

  override val stub = classOf[SequentialInputFormat]

  override def persistConfiguration(config: Configuration) {
    if (blockSize.isDefined)
      config.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, blockSize.get)
  }
}

case class DelimetedDataSourceFormat[Out: UDT](val readFunction: (Array[Byte], Int, Int) => Out, val delimeter: Option[String] = None) extends DataSourceFormat[Out] {

  def this(readFunction: (Array[Byte], Int, Int) => Out, delimeter: String) = this(readFunction, Some(delimeter))

  override val stub = classOf[DelimetedInput4sStub[Out]]

  override def persistConfiguration(config: Configuration) {

    val serializer = outputUDT.getSerializer(fieldSelector.getFields)

    val stubParameters = DelimetedInputParameters(serializer, readFunction)
    stubParameters.persist(config)

    if (delimeter.isDefined)
      config.setString(DelimitedInputFormat.RECORD_DELIMITER, delimeter.get)
  }
}

object DelimetedDataSourceFormat {

  def apply[Out: UDT](parseFunction: String => Out): DelimetedDataSourceFormat[Out] = DelimetedDataSourceFormat(asReadFunction(parseFunction) _, None)
  def apply[Out: UDT](parseFunction: String => Out, delimeter: String): DelimetedDataSourceFormat[Out] = DelimetedDataSourceFormat(asReadFunction(parseFunction) _, Some(delimeter))

  private def asReadFunction[Out: UDT](parseFunction: String => Out)(source: Array[Byte], offset: Int, numBytes: Int): Out = {

    parseFunction(new String(source, offset, numBytes))
  }
}

case class RecordDataSourceFormat[Out: UDT](val recordDelimeter: Option[String] = None, val fieldDelimeter: Option[String] = None) extends DataSourceFormat[Out] {

  def this(recordDelimeter: String, fieldDelimeter: String) = this(Some(recordDelimeter), Some(fieldDelimeter))

  override val stub = classOf[RecordInputFormat]

  override def persistConfiguration(config: Configuration) {

    val fields = outputUDT.fieldTypes

    config.setInteger(RecordInputFormat.NUM_FIELDS_PARAMETER, fields.length)

    for (fieldNum <- 0 until fields.length)
      config.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + fieldNum, fieldParserTypes(fields(fieldNum)))

    if (recordDelimeter.isDefined)
      config.setString(RecordInputFormat.RECORD_DELIMITER_PARAMETER, recordDelimeter.get)

    if (fieldDelimeter.isDefined)
      config.setString(RecordInputFormat.FIELD_DELIMITER_PARAMETER, fieldDelimeter.get)
  }

  private val fieldParserTypes: Map[Class[_ <: PactValue], Class[_ <: FieldParser[_]]] = Map(
    classOf[PactDouble] -> classOf[DecimalTextDoubleParser],
    classOf[PactInteger] -> classOf[DecimalTextIntParser],
    classOf[PactLong] -> classOf[DecimalTextLongParser],
    classOf[PactString] -> classOf[VarLengthStringParser])
}

case class TextDataSourceFormat(val charSetName: Option[String] = None) extends DataSourceFormat[String] {

  def this(charSetName: String) = this(Some(charSetName))

  override val stub = classOf[TextInputFormat]

  override def persistConfiguration(config: Configuration) {
    if (charSetName.isDefined)
      config.setString(TextInputFormat.CHARSET_NAME, charSetName.get)
  }
}

case class FixedLengthDataSourceFormat[Out: UDT](val readFunction: (Array[Byte], Int) => Out, val recordLength: Int) extends DataSourceFormat[Out] {

  override val stub = classOf[FixedLengthInput4sStub[Out]]

  override def persistConfiguration(config: Configuration) {
    val serializer = outputUDT.getSerializer(fieldSelector.getFields)

    val stubParameters = FixedLengthInputParameters(serializer, readFunction)
    stubParameters.persist(config)

    config.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, recordLength)
  }
}

case class ExternalProcessFixedLengthDataSourceFormat[Out: UDT](val readFunction: (Array[Byte], Int) => Out, val recordLength: Int, val externalProcessCommand: Int => String, val numSplits: Option[Int] = None, val exitCodes: Option[Seq[Int]] = None) extends DataSourceFormat[Out] {

  def this(readFunction: (Array[Byte], Int) => Out, recordLength: Int, externalProcessCommand: Int => String, exitCodes: Seq[Int]) = this(readFunction, recordLength, externalProcessCommand, None, Some(exitCodes))

  override val stub = classOf[ExternalProcessFixedLengthInput4sStub[Out]]

  override def persistConfiguration(config: Configuration) {
    val serializer = outputUDT.getSerializer(fieldSelector.getFields)

    val stubParameters = ExternalProcessFixedLengthInputParameters(serializer, externalProcessCommand, numSplits, readFunction)
    stubParameters.persist(config)

    config.setInteger(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, recordLength)

    if (exitCodes.isDefined)
      config.setString(ExternalProcessInputFormat.ALLOWEDEXITCODES_PARAMETER_KEY, exitCodes.get.mkString(","))
  }
}