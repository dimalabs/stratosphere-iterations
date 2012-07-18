/**
 * *********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * ********************************************************************************************************************
 */

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