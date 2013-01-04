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

package eu.stratosphere.pact4s.common.contracts

import scala.collection.mutable
import scala.util.DynamicVariable

import eu.stratosphere.pact4s.common.HasHints

import eu.stratosphere.pact.common.contract.Contract

trait Pact4sContractFactory { this: HasHints[_] =>

  protected def createContract: Contract

  def getContract: Contract = Pact4sContractFactory.currentEnv.value.getContractFor(this, initContract)

  private def initContract: Contract = {
    val c = createContract
    for (hint <- getHints)
      hint.applyToContract(c)
    c
  }
}

object Pact4sContractFactory {

  private class Environment {

    private val contracts = mutable.Map[Pact4sContractFactory, Contract]()

    def getContractFor(factory: Pact4sContractFactory, instance: => Contract): Contract = {
      contracts.getOrElseUpdate(factory, instance)
    }
  }

  private val currentEnv: DynamicVariable[Environment] = new DynamicVariable(null)
  
  def withEnvironment[T](thunk: => T): T = currentEnv.value match {
    case null => currentEnv.withValue(new Environment) { thunk }
    case _    => thunk
  }
}