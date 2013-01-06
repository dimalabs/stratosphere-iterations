package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.nephele.configuration.Configuration

case class CopyParameters(
  val from: Array[Int], 
  val to: Array[Int],  
  val types: Array[Class[_ <: PactValue]],
  val discard: Array[Int]) 
  extends StubParameters

class Copy4sStub extends MapStub {

  private var from: Array[Int] = _
  private var to: Array[Int] = _
  private var types: Array[Class[_ <: PactValue]] = _
  private var discard: Array[Int] = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[CopyParameters](config)

    this.from = parameters.from
    this.to = parameters.to
    this.types = parameters.types
    this.discard = parameters.discard
  }

  override def map(record: PactRecord, out: Collector[PactRecord]) = {
    
    var field = 0
    while (field < from.length) {
      record.setField(to(field), record.getField(from(field), types(field)))
      field = field + 1
    }

    for (field <- discard)
      record.setNull(field)
      
    out.collect(record)
  }
}