package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait Join4sContract[Key, LeftIn, RightIn, Out] extends Pact4sContract
  with KeyedTwoInputContract[Key, LeftIn, RightIn] { this: MatchContract =>

  def getStubParameters: JoinParameters[LeftIn, RightIn, Out]

  override def persistConfiguration() = {
    StubParameters.setValue(this, getStubParameters)
  }
}
