package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.Hintable
import eu.stratosphere.pact4s.common.analyzer.UDT

abstract class DataStream[T: UDT] extends Hintable {

  protected def contract: Pact4sContract

  def getContract: Pact4sContract = {
    val c = contract
    this.applyHints(c)
    c
  }
}
