package eu.stratosphere.pact4s.common.analyzer

import scala.collection.mutable

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.contracts.Pact4sContract
import eu.stratosphere.pact4s.common.contracts.Pact4sContractFactory

import eu.stratosphere.pact.common.contract.Contract

class Environment {

  private val contracts = mutable.Map[Pact4sContractFactory, Contract]()
  private val outDegrees = mutable.Map[Contract, Int]()

  def getContractFor(factory: Pact4sContractFactory, instance: => Contract): Contract = {

    val inst = contracts.getOrElseUpdate(factory, instance)
    outDegrees(inst) = outDegrees.getOrElse(inst, 0) + 1
    inst
  }
  
  def getOutDegree(contract: Contract) = outDegrees(contract)
}