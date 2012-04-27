package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class CrossParameters(
  val leftDeserializer: UDTSerializer[Any],
  val rightDeserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val mapFunction: (Any, Any) => Any)
  extends StubParameters

case class FlatCrossParameters(
  val leftDeserializer: UDTSerializer[Any],
  val rightDeserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val mapFunction: (Any, Any) => Iterator[Any])
  extends StubParameters