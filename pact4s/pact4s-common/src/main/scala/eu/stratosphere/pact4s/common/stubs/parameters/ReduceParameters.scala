package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class ReduceParameters[Key, In, Out](
  val inputUDT: UDT[In],
  val outputUDT: UDT[Out],
  val keySelector: KeySelector[In => Key],
  val combineUDF: Option[UDF1[Iterator[In] => In]],
  val combineFunction: Option[Iterator[In] => In],
  val reduceUDF: UDF1[Iterator[In] => Out],
  val reduceFunction: Iterator[In] => Out)
  extends StubParameters