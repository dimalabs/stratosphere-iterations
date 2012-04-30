package eu.stratosphere.pact4s.common.stubs

import java.io.DataOutput
import java.io.OutputStream

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.`type`.PactRecord

class RawOutput4sStub[In] extends FileOutputFormat with ParameterizedOutputFormat[RawOutputParameters[In]] {

  private var deserializer: UDTSerializer[In] = _
  private var writeFunction: (In, OutputStream) => Unit = _

  override def initialize(parameters: RawOutputParameters[In]) = {
    val RawOutputParameters(inputUDT, writeUDF, formatFunction) = parameters

    this.deserializer = inputUDT.createSerializer(writeUDF.getFields)
    this.writeFunction = writeFunction
  }

  override def writeRecord(record: PactRecord) = {

    val input = deserializer.deserialize(record)
    writeFunction.apply(input, this.stream)
  }
}

class BinaryOutput4sStub[In] extends BinaryOutputFormat with ParameterizedOutputFormat[BinaryOutputParameters[In]] {

  private var deserializer: UDTSerializer[In] = _
  private var writeFunction: (In, DataOutput) => Unit = _

  override def initialize(parameters: BinaryOutputParameters[In]) = {
    val BinaryOutputParameters(inputUDT, writeUDF, formatFunction) = parameters

    this.deserializer = inputUDT.createSerializer(writeUDF.getFields)
    this.writeFunction = writeFunction
  }

  override def serialize(record: PactRecord, target: DataOutput) = {

    val input = deserializer.deserialize(record)
    writeFunction.apply(input, target)
  }
}

class DelimetedOutput4sStub[In] extends DelimitedOutputFormat with ParameterizedOutputFormat[DelimetedOutputParameters[In]] {

  private var deserializer: UDTSerializer[In] = _
  private var writeFunction: (In, Array[Byte]) => Int = _

  override def initialize(parameters: DelimetedOutputParameters[In]) = {
    val DelimetedOutputParameters(inputUDT, writeUDF, formatFunction) = parameters

    this.deserializer = inputUDT.createSerializer(writeUDF.getFields)
    this.writeFunction = writeFunction
  }

  override def serializeRecord(record: PactRecord, target: Array[Byte]): Int = {

    val input = deserializer.deserialize(record)
    writeFunction.apply(input, target)
  }
}