package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class CoGroupParameters[LeftIn, RightIn, Out](
  val leftDeserializer: UDTSerializer[LeftIn],
  val leftForward: Array[Int],
  val rightDeserializer: UDTSerializer[RightIn],
  val rightForward: Array[Int],
  val serializer: UDTSerializer[Out],
  val mapFunction: Either[(Iterator[LeftIn], Iterator[RightIn]) => Out, (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]])
  extends StubParameters
