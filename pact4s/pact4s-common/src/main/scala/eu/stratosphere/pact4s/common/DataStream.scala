package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analyzer.UDT
import eu.stratosphere.pact4s.common.contracts.Pact4sContract

abstract class DataStream[T: UDT] extends Hintable[T] {

  protected def contract: Pact4sContract

  def getContract: Pact4sContract = {
    val c = contract
    this.applyHints(c)
    c
  }
}
