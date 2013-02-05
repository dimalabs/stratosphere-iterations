/**
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
 */

package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class RepeatOperator[SolutionItem: UDT](stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]) extends Serializable {

  // This is undesirable for a huge number of iterations since it will cause
  // the job graph to explode. But until PACT iterations are fully implemented,
  // this will at least get small example programs up and running.
  def repeat(n: Int, s0: DataStream[SolutionItem]): DataStream[SolutionItem] = Function.chain(List.fill(n)(stepFunction))(s0)

  def chain(n: Int, s0: DataStream[SolutionItem]) = new DataStream[SolutionItem] with OutputHintable[SolutionItem] {

    override def createContract = {

      val contract = new Iteration with Iterate4sContract[SolutionItem] {
        override val udf = new UDF0[SolutionItem]
      }

      val solutionInput = new DataStream[SolutionItem] {
        override def createContract = contract.getPartialSolution()
      }

      val output = stepFunction(solutionInput)

      contract.setInitialPartialSolution(s0.getContract)
      contract.setNextPartialSolution(output.getContract)
      contract.setNumberOfIteration(n)

      applyHints(contract)
      contract
    }
  }
}

class IterateOperator[SolutionItem: UDT, DeltaItem: UDT](stepFunction: DataStream[SolutionItem] => (DataStream[SolutionItem], DataStream[DeltaItem])) extends Serializable {

  def iterate(s0: DataStream[SolutionItem]) = new DataStream[SolutionItem] with OutputHintable[SolutionItem] {

    override def createContract = {

      val outer = this

      val contract = new Iteration with Iterate4sContract[SolutionItem] {
        override val udf = new UDF0[SolutionItem]
      }

      val solutionInput = new DataStream[SolutionItem] {
        override def createContract = contract.getPartialSolution()
      }

      val (output, term) = stepFunction(solutionInput)

      contract.setInitialPartialSolution(s0.getContract)
      contract.setNextPartialSolution(output.getContract)

      if (term != null) contract.setTerminationCriterion(term.getContract)

      applyHints(contract)
      contract
    }
  }
}

class WorksetIterateOperator[SolutionItem: UDT, WorksetItem: UDT](stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) extends Serializable {

  def iterate[SolutionKey](s0: DistinctDataStream[SolutionItem, SolutionKey], ws0: DataStream[WorksetItem]) = new DataStream[SolutionItem] with OutputHintable[SolutionItem] {

    override def createContract = {

      val keyFields = s0.keySelector.selectedFields
      val keyTypes = implicitly[UDT[SolutionItem]].getKeySet(keyFields map { _.localPos })
      val keyPositions = keyFields map { _ => -1 } toArray

      val contract = new WorksetIteration(keyTypes, keyPositions) with WorksetIterate4sContract[SolutionKey, SolutionItem, WorksetItem] {
        override val key = s0.keySelector.copy()
        override val udf = new UDF0[SolutionItem]
      }

      val solutionInput = new DataStream[SolutionItem] {
        override def createContract = contract.getPartialSolution()
      }

      val worksetInput = new DataStream[WorksetItem] {
        override def createContract = contract.getWorkset()
      }

      contract.setInitialPartialSolution(s0.stream.getContract)
      contract.setInitialWorkset(ws0.getContract)

      val (delta, nextWorkset) = stepFunction(solutionInput, worksetInput)
      contract.setPartialSolutionDelta(delta.getContract)
      contract.setNextWorkset(nextWorkset.getContract)

      applyHints(contract)
      contract
    }
  }
}

class DistinctByOperator[T: UDT](stream: DataStream[T]) extends Serializable {
  def distinctBy[Key](keySelector: KeySelector[T => Key]) = new DistinctDataStream(stream, keySelector)
}

case class DistinctDataStream[T, Key](stream: DataStream[T], keySelector: KeySelector[T => Key])

