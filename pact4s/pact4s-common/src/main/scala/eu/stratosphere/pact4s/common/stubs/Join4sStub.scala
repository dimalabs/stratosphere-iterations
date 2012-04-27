package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord

class Join4sStub extends MatchStub with ParameterizedStub[JoinParameters] {

  override def `match`(left: PactRecord, right: PactRecord, out: Collector) {

  }
}

class FlatJoin4sStub extends MatchStub with ParameterizedStub[FlatJoinParameters] {

  override def `match`(left: PactRecord, right: PactRecord, out: Collector) {

  }
}