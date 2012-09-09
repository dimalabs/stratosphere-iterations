package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class CopyParameters(
  val from: Array[Int], 
  val to: Array[Int],  
  val discard: Array[Int]) 
  extends StubParameters

class Copy4sStub extends MapStub {

  private var from: Array[Int] = _
  private var to: Array[Int] = _
  private var discard: Array[Int] = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[CopyParameters](config)

    this.from = parameters.from
    this.to = parameters.to
    this.discard = parameters.discard
  }

  override def map(record: PactRecord, out: Collector[PactRecord]) = {
    record.copyFrom(record, from, to);
    for (field <- discard)
      record.setNull(field)
    out.collect(record)
  }
}