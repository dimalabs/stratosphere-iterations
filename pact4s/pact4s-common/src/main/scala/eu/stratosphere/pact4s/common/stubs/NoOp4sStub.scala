package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

class NoOp4sStub extends MapStub {
  override def map(record: PactRecord, out: Collector[PactRecord]) = out.collect(record)
}