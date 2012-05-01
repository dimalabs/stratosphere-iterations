package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait Reduce4sContract[Key, In, Out] extends Pact4sContract
  with KeyedOneInputContract[Key, In] { this: ReduceContract =>

  def getStubParameters: ReduceParameters[In, Out]

  override def persistConfiguration() = {
    StubParameters.setValue(this, getStubParameters)
  }
}
