package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class CrossParameters[LeftIn, RightIn, Out](
  val leftDeserializer: UDTSerializer[LeftIn],
  val leftDiscard: Array[Int],
  val rightDeserializer: UDTSerializer[RightIn],
  val rightDiscard: Array[Int],
  val serializer: UDTSerializer[Out],
  val mapFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]])
  extends StubParameters
