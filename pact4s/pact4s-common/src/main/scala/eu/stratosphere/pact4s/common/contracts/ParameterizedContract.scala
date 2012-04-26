package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.stubs.parameters.StubParameters

import eu.stratosphere.pact.common.contract.Contract

trait ParameterizedContract[T <: StubParameters] extends FinalizableContract { this: Contract =>

  def getStubParameters: T

  override def persistConfiguration() {
    StubParameters.setValue(this, getStubParameters)
  }
}