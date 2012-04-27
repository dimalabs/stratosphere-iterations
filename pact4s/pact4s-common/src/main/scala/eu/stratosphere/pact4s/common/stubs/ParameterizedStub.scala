package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.stubs.parameters.StubParameters

import eu.stratosphere.pact.common.stubs.Stub
import eu.stratosphere.nephele.configuration.Configuration

trait ParameterizedStub[T <: StubParameters] { this: Stub =>

  def initialize(parameters: T) = {}

  override def open(config: Configuration) {
    initialize(StubParameters.getValue[T](config))
  }
}