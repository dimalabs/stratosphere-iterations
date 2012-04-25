package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact.common.contract.Contract

trait Pact4sContract { this: Contract =>
  def persistConfiguration()
}

object Pact4sContract {
  implicit def pact4sContract2Contract(c: Pact4sContract): Contract = c
}