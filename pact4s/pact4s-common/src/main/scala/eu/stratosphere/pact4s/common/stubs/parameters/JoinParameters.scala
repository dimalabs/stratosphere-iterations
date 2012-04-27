package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class JoinParameters(
  val leftDeserializer: UDTSerializer[Any],
  val rightDeserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val leftCopyKeys: Array[Int],
  val rightCopyKeys: Array[Int],
  val mapFunction: (Any, Any) => Any)
  extends StubParameters

case class FlatJoinParameters(
  val leftDeserializer: UDTSerializer[Any],
  val rightDeserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val leftCopyKeys: Array[Int],
  val rightCopyKeys: Array[Int],
  val mapFunction: (Any, Any) => Iterator[Any])
  extends StubParameters