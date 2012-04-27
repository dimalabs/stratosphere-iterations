package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord

class Cross4sStub extends CrossStub with ParameterizedStub[CrossParameters] {

  override def cross(left: PactRecord, right: PactRecord, out: Collector) {

  }
}

class FlatCross4sStub extends CrossStub with ParameterizedStub[FlatCrossParameters] {

  override def cross(left: PactRecord, right: PactRecord, out: Collector) {

  }
}