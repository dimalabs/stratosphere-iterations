package eu.stratosphere.pact4s.common.analyzer

import scala.collection.mutable

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.contracts.Pact4sContract
import eu.stratosphere.pact4s.common.contracts.Pact4sContractFactory

class Environment {

  private val contracts = mutable.Map[Pact4sContractFactory, Pact4sContract]()

  def getContractFor(factory: Pact4sContractFactory, instance: => Pact4sContract): Pact4sContract = {

    val inst = contracts.getOrElseUpdate(factory, instance)
    inst.outDegree += 1
    inst
  }
}