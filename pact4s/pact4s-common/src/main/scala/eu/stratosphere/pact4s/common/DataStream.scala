package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analyzer.UDT
import eu.stratosphere.pact4s.common.contracts.Pact4sContract

abstract class DataStream[T: UDT] extends Hintable[T] {

  protected def createContract: Pact4sContract

  lazy val getContract: Pact4sContract = {

    val contract = createContract
    this.applyHints(contract)
    contract
  }
}
