package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class MapParameters[In, Out](
  val deserializer: UDTSerializer[In],
  val serializer: UDTSerializer[Out],
  val discard: Array[Int],
  val userFunction: Either[In => Out, In => Iterator[Out]])
  extends StubParameters

class Map4sStub[In, Out] extends MapStub {

  private var deserializer: UDTSerializer[In] = _
  private var serializer: UDTSerializer[Out] = _
  private var discard: Array[Int] = _

  private var userFunction: (PactRecord, Collector) => Unit = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[MapParameters[In, Out]](config)

    this.deserializer = parameters.deserializer
    this.serializer = parameters.serializer
    this.discard = parameters.discard

    this.userFunction = parameters.userFunction.fold(doMap _, doFlatMap _)
  }

  override def map(record: PactRecord, out: Collector) = userFunction(record, out)

  private def doMap(userFunction: In => Out)(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = userFunction.apply(input)

    for (field <- discard)
      record.setNull(field)

    serializer.serialize(output, record)
    out.collect(record)
  }

  private def doFlatMap(userFunction: In => Iterator[Out])(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = userFunction.apply(input)

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
