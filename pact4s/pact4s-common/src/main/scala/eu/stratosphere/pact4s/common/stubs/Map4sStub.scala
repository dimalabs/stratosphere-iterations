package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.stubs.StubAnnotation._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode
import eu.stratosphere.pact.common.`type`.PactRecord

@ImplicitOperation(implicitOperation = ImplicitOperationMode.Copy)
class Map4sStub[In, Out] extends MapStub with ParameterizedStub[MapParameters[In, Out]] {

  private var deserializer: UDTSerializer[In] = _
  private var discardedFields: Iterable[Int] = _
  private var mapFunction: In => Out = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: MapParameters[In, Out]) {
    val MapParameters(inputUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.deserializer = inputUDT.createSerializer(mapUDF.getReadFields)
    this.discardedFields = mapUDF.getDiscardedFields
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def map(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = mapFunction.apply(input)

    for (field <- discardedFields)
      record.setNull(field)

    serializer.serialize(output, record)
    out.collect(record)
  }
}

@ImplicitOperation(implicitOperation = ImplicitOperationMode.Copy)
class FlatMap4sStub[In, Out] extends MapStub with ParameterizedStub[FlatMapParameters[In, Out]] {

  private var deserializer: UDTSerializer[In] = _
  private var discardedFields: Iterable[Int] = _
  private var mapFunction: In => Iterator[Out] = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: FlatMapParameters[In, Out]) {
    val FlatMapParameters(inputUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.deserializer = inputUDT.createSerializer(mapUDF.getReadFields)
    this.discardedFields = mapUDF.getDiscardedFields
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def map(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = mapFunction.apply(input)

    for (field <- discardedFields)
      record.setNull(field)

    for (item <- output) {
      serializer.serialize(item, record)
      out.collect(record)
    }
  }
}
