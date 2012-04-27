package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class MapParameters[In, Out](
  val inputUDT: UDT[In],
  val outputUDT: UDT[Out],
  val mapUDF: UDF1[In => Out],
  val mapFunction: In => Out)
  extends StubParameters {

  def createDeserializer() = inputUDT.createSerializer(mapUDF.readFields)
  def createSerializer() = outputUDT.createSerializer(mapUDF.writeFields)
}

case class FlatMapParameters[In, Out](
  val inputUDT: UDT[In],
  val outputUDT: UDT[Out],
  val mapUDF: UDF1[In => Iterator[Out]],
  val mapFunction: In => Iterator[Out])
  extends StubParameters {

  def createDeserializer() = inputUDT.createSerializer(mapUDF.readFields)
  def createSerializer() = outputUDT.createSerializer(mapUDF.writeFields)
}
