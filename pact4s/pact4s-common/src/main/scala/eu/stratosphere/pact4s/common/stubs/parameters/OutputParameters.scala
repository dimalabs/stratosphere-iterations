package eu.stratosphere.pact4s.common.stubs.parameters

import java.io.DataOutput
import java.io.OutputStream

import eu.stratosphere.pact4s.common.analyzer._

case class RawOutputParameters[In](
  val inputUDT: UDT[In],
  val writeUDF: FieldSelector[In => Unit],
  val writeFunction: (In, OutputStream) => Unit)
  extends StubParameters

case class BinaryOutputParameters[In](
  val inputUDT: UDT[In],
  val writeUDF: FieldSelector[In => Unit],
  val writeFunction: (In, DataOutput) => Unit)
  extends StubParameters

case class DelimetedOutputParameters[In](
  val inputUDT: UDT[In],
  val writeUDF: FieldSelector[In => Unit],
  val writeFunction: (In, Array[Byte]) => Int)
  extends StubParameters
