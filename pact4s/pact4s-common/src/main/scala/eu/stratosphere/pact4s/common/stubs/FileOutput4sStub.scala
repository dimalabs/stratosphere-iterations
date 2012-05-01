package eu.stratosphere.pact4s.common.stubs

import java.io.DataOutput
import java.io.OutputStream

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class RawOutputParameters[In](
  val deserializer: UDTSerializer[In],
  val userFunction: (In, OutputStream) => Unit)
  extends StubParameters

case class BinaryOutputParameters[In](
  val deserializer: UDTSerializer[In],
  val userFunction: (In, DataOutput) => Unit)
  extends StubParameters

case class DelimetedOutputParameters[In](
  val deserializer: UDTSerializer[In],
  val userFunction: (In, Array[Byte]) => Int)
  extends StubParameters

class RawOutput4sStub[In] extends FileOutputFormat {

  private var deserializer: UDTSerializer[In] = _
  private var userFunction: (In, OutputStream) => Unit = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[RawOutputParameters[In]](config)

    this.deserializer = parameters.deserializer
    this.userFunction = parameters.userFunction
  }

  override def writeRecord(record: PactRecord) = {

    val input = deserializer.deserialize(record)
    userFunction.apply(input, this.stream)
  }
}

class BinaryOutput4sStub[In] extends BinaryOutputFormat {

  private var deserializer: UDTSerializer[In] = _
  private var userFunction: (In, DataOutput) => Unit = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[BinaryOutputParameters[In]](config)

    this.deserializer = parameters.deserializer
    this.userFunction = parameters.userFunction
  }

  override def serialize(record: PactRecord, target: DataOutput) = {

    val input = deserializer.deserialize(record)
    userFunction.apply(input, target)
  }
}

class DelimetedOutput4sStub[In] extends DelimitedOutputFormat {

  private var deserializer: UDTSerializer[In] = _
  private var userFunction: (In, Array[Byte]) => Int = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[DelimetedOutputParameters[In]](config)

    this.deserializer = parameters.deserializer
    this.userFunction = parameters.userFunction
  }

  override def serializeRecord(record: PactRecord, target: Array[Byte]): Int = {

    val input = deserializer.deserialize(record)
    userFunction.apply(input, target)
  }
}