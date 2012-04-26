package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact.common.contract.Contract

trait FinalizableContract { this: Contract =>
  def persistConfiguration()
}

object FinalizableContract {
  implicit def toContract(c: FinalizableContract): Contract = c
}
