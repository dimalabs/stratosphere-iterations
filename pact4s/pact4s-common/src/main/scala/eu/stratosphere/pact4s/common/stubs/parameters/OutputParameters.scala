package eu.stratosphere.pact4s.common.stubs.parameters

import java.io.OutputStream

import eu.stratosphere.pact4s.common.analyzer._

case class OutputParameters[In](
  val inputUDT: UDT[In],
  val writeUDF: UDF2[(In, OutputStream) => Unit],
  val writeFunction: (In, OutputStream) => Unit)
  extends StubParameters