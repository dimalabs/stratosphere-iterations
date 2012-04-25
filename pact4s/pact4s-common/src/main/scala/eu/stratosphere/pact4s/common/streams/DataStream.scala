package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.Hintable
import eu.stratosphere.pact4s.common.analyzer.UDT

import eu.stratosphere.pact.common.contract.Contract

abstract class DataStream[T: UDT] extends Hintable {
  val udt = implicitly[UDT[T]]

  def getContract: Contract
}
