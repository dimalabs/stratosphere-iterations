package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class ReduceParameters[In, Out](
  val combineDeserializer: Option[UDTSerializer[In]],
  val combineSerializer: Option[UDTSerializer[In]],
  val combineForward: Option[Array[Int]],
  val combineFunction: Option[Iterator[In] => In],
  val reduceDeserializer: UDTSerializer[In],
  val reduceSerializer: UDTSerializer[Out],
  val reduceForward: Array[Int],
  val reduceFunction: Iterator[In] => Out)
  extends StubParameters
