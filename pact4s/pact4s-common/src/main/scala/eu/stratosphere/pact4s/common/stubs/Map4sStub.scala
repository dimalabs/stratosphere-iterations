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
  private var serializer: UDTSerializer[Out] = _
  private var discard: Array[Int] = _

  private var stubFunction: (PactRecord, Collector) => Unit = _

  override def initialize(parameters: MapParameters[In, Out]) {

    this.deserializer = parameters.deserializer
    this.serializer = parameters.serializer
    this.discard = parameters.discard

    this.stubFunction = parameters.mapFunction.fold(doMap _, doFlatMap _)
  }

  override def map(record: PactRecord, out: Collector) = stubFunction(record, out)

  private def doMap(mapFunction: In => Out)(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = mapFunction.apply(input)

    for (field <- discard)
      record.setNull(field)

    serializer.serialize(output, record)
    out.collect(record)
  }

  private def doFlatMap(flatMapFunction: In => Iterator[Out])(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = flatMapFunction.apply(input)

    if (output.nonEmpty) {

      for (field <- discard)
        record.setNull(field)

      for (item <- output) {
        serializer.serialize(item, record)
        out.collect(record)
      }
    }
  }
}
