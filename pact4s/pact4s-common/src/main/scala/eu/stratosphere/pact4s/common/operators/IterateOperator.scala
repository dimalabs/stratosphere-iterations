package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class RepeatOperator[SolutionItem: UDT](stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]) extends Serializable {

  def ^(numIterations: Int): DataStream[SolutionItem] => DataStream[SolutionItem] = Function.chain(List.fill(numIterations)(stepFunction))

  /*
  def ^(numIterations: Int): DataStream[SolutionItem] => DataStream[SolutionItem] = (initialSolution: DataStream[SolutionItem]) => new DataStream[SolutionItem] {

    override def createContract = {

      val contract = new Iteration with Iterate4sContract[SolutionItem]

      val solutionInput = new DataStream[SolutionItem] {
        override def createContract = contract.getPartialSolution()
      }

      val output = stepFunction(solutionInput)

      contract.setInitialPartialSolution(initialSolution.getContract)
      contract.setNextPartialSolution(output.getContract)
      contract.setNumberOfIteration(numIterations)

      contract
    }
  }
  */
}

class IterateOperator[SolutionItem: UDT, DeltaItem: UDT](stepFunction: DataStream[SolutionItem] => (DataStream[SolutionItem], DataStream[DeltaItem])) extends Serializable {

  def iterate(s0: DataStream[SolutionItem]) = new DataStream[SolutionItem] {

    override def createContract = {

      val contract = new Iteration with Iterate4sContract[SolutionItem]

      val solutionInput = new DataStream[SolutionItem] {
        override def createContract = contract.getPartialSolution()
      }

      val (output, term) = stepFunction(solutionInput)

      contract.setInitialPartialSolution(s0.getContract)
      contract.setNextPartialSolution(output.getContract)
      contract.setTerminationCriterion(term.getContract)

      contract
    }
  }
}

class WorksetIterateOperator[SolutionItem: UDT, WorksetItem: UDT](stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) extends Serializable {

  def iterate[Key, SolutionKeySelector: SelectorBuilder[SolutionItem, Key]#Selector](s0: DistinctDataStream[SolutionItem, Key, SolutionKeySelector], ws0: DataStream[WorksetItem]) = new DataStream[SolutionItem] {

    override def createContract = {

      val keyFieldSelector = implicitly[FieldSelector[SolutionItem => Key]]
      val keyFields = keyFieldSelector.getFields filter { _ >= 0 }
      val keyFieldTypes = implicitly[UDT[SolutionItem]].getKeySet(keyFields)

      val contract = new WorksetIteration with WorksetIterate4sContract[Key, SolutionItem, WorksetItem] {

        override val keySelector = keyFieldSelector
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

      contract
    }
  }
}

class DistinctByOperator[T: UDT](stream: DataStream[T]) {
  def distinctBy[Key, SolutionKeySelector: SelectorBuilder[T, Key]#Selector](keySelector: T => Key) = new DistinctDataStream(stream, keySelector)
}

class DistinctDataStream[T: UDT, Key, SolutionKeySelector: SelectorBuilder[T, Key]#Selector](val stream: DataStream[T], val keySelector: T => Key)

