package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait Cross4sContract[LeftIn, RightIn, Out] extends Pact4sContract { this: CrossContract =>

  def getStubParameters: CrossParameters[LeftIn, RightIn, Out]

  override def persistConfiguration() = {
    StubParameters.setValue(this, getStubParameters)
  }
}
