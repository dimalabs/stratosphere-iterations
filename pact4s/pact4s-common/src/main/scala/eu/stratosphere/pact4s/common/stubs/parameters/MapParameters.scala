package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class MapParameters(
  val deserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val mapFunction: Any => Any)
  extends StubParameters

case class FlatMapParameters(
  val deserializer: UDTSerializer[Any],
  val serializer: UDTSerializer[Any],
  val mapFunction: Any => Iterator[Any])
  extends StubParameters
