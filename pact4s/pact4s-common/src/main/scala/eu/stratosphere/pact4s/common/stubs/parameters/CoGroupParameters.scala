package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class CoGroupParameters(
  val leftDeserializer: UDTSerializer[Any],
  val rightDeserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val leftCopyKeys: Array[Int],
  val rightCopyKeys: Array[Int],
  val mapFunction: (Iterator[Any], Iterator[Any]) => Any)
  extends StubParameters

case class FlatCoGroupParameters(
  val leftDeserializer: UDTSerializer[Any],
  val rightDeserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val leftCopyKeys: Array[Int],
  val rightCopyKeys: Array[Int],
  val mapFunction: (Iterator[Any], Iterator[Any]) => Iterator[Any])
  extends StubParameters