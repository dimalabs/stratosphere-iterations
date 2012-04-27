package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.Hintable
import eu.stratosphere.pact4s.common.analyzer.UDT
import eu.stratosphere.pact4s.common.stubs.parameters.StubParameters

import eu.stratosphere.pact.common.contract.Contract

abstract class DataStream[T: UDT] extends Hintable {

  protected def contract: FinalizableContract

  def getContract: FinalizableContract = {
    val c = contract
    this.applyHints(c)
    c
  }
}

trait FinalizableContract { this: Contract =>
  def persistConfiguration()
}

object FinalizableContract {
  implicit def toContract(c: FinalizableContract): Contract = c
}

trait ParameterizedContract[T <: StubParameters] extends FinalizableContract { this: Contract =>

  def getStubParameters: T

  override def persistConfiguration() {
    StubParameters.setValue(this, getStubParameters)
  }
}