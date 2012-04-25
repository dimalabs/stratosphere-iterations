package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact.common.contract.Contract

trait Pact4sContract { this: Contract =>
  def persistConfiguration()
}
