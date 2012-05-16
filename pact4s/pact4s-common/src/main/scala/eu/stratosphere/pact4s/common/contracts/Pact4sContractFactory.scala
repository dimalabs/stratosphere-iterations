package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.CompilerHint
import eu.stratosphere.pact4s.common.analyzer.Environment

import eu.stratosphere.pact.common.contract.Contract

trait Pact4sContractFactory {

  protected def getHints: Seq[CompilerHint[_]]
  protected def createContract: Contract

  private var env: Environment = _
  protected implicit def getEnv = env

  def getContract(implicit env: Environment): Contract = {
    this.env = env
    val contract = env.getContractFor(this, initContract)
    this.env = null
    contract
  }

  private def initContract: Contract = {
    val c = createContract
    for (hint <- getHints)
      hint.applyToContract(c)
    c
  }
}