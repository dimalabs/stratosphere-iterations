package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.stubs.parameters.StubParameters

import eu.stratosphere.pact.common.stubs.Stub
import eu.stratosphere.nephele.configuration.Configuration

trait ParameterizedStub[T <: StubParameters] { this: Stub =>

  private var parameters: T = _

  def getParameters = parameters

  override def open(config: Configuration) {
    parameters = StubParameters.getValue[T](config)
  }
}