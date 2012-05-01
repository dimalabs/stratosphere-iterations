package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class MapParameters[In, Out](
  val deserializer: UDTSerializer[In],
  val serializer: UDTSerializer[Out],
  val discard: Array[Int],
  val mapFunction: Either[In => Out, In => Iterator[Out]])
  extends StubParameters
