package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class ReduceParameters(
  val combinerSerializer: Option[UDTSerializer[Any]],
  val deserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val copyKeys: Array[Int],
  val combineFunction: Option[Iterator[Any] => Any],
  val reduceFunction: Iterator[Any] => Any)
  extends StubParameters