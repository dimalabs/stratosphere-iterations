package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.stubs.parameters.StubParameters

import eu.stratosphere.pact.common.stubs.Stub
import eu.stratosphere.pact.common.io.OutputFormat
import eu.stratosphere.nephele.configuration.Configuration

trait ParameterizedStub[T <: StubParameters] extends Stub {

  def initialize(parameters: T) = {}

  abstract override def open(config: Configuration) {
    super.open(config)
    initialize(StubParameters.getValue[T](config))
  }
}

trait ParameterizedOutputFormat[T <: StubParameters] extends OutputFormat {

  def initialize(parameters: T) = {}

  abstract override def configure(config: Configuration) {
    super.configure(config)
    initialize(StubParameters.getValue[T](config))
  }
}