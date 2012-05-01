package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait Map4sContract[In, Out] extends Pact4sContract { this: MapContract =>

  def getStubParameters: MapParameters[In, Out]

  override def persistConfiguration() = {
    StubParameters.setValue(this, getStubParameters)
  }
}
