package eu.stratosphere.pact4s.common.stubs

import java.io.OutputStream

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.io.FileOutputFormat
import eu.stratosphere.pact.common.`type`.PactRecord

class File4sOutputStub[In] extends FileOutputFormat with ParameterizedOutputFormat[OutputParameters[In]] {

  private var deserializer: UDTSerializer[In] = _
  private var writeFunction: (In, OutputStream) => Unit = _

  override def initialize(parameters: OutputParameters[In]) {
    val OutputParameters(inputUDT, writeUDF, formatFunction) = parameters

    this.deserializer = inputUDT.createSerializer(writeUDF.getReadFields._1)
    this.writeFunction = writeFunction
  }

  override def writeRecord(record: PactRecord) {

    val input = deserializer.deserialize(record)
    writeFunction.apply(input, this.stream)
  }
}