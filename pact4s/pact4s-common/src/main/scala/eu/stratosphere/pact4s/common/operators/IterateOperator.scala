package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class IterateOperator[SolutionItem: UDT](initialSolution: DataStream[SolutionItem]) extends Serializable {

  def iterate(stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]): DataStream[SolutionItem] = new DataStream[SolutionItem] {

    override def createContract = new Iteration with Pact4sContract {
      
      private val solutionInput = new DataStream[SolutionItem] {
        override def createContract = getPartialSolution()
      }
      
      this.setInitialPartialSolution(initialSolution.getContract)
      this.setNextPartialSolution(stepFunction(solutionInput).getContract)
    } 
  }

  def keyBy[Key, SolutionKeySelector: SelectorBuilder[SolutionItem, Key]#Selector](keySelector: SolutionItem => Key) = new Serializable {

    def untilEmpty[WorksetItem: UDT](initialWorkset: DataStream[WorksetItem]) = new Serializable {

      def iterate(stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])): DataStream[SolutionItem] = new DataStream[SolutionItem] {

        override def createContract = new WorksetIteration with Pact4sContract {
          
          private val solutionInput = new DataStream[SolutionItem] {
            override def createContract = getPartialSolution()
          }

          private val worksetInput = new DataStream[WorksetItem] {
            override def createContract = getWorkset()
          }
          
          this.setInitialPartialSolution(initialSolution.getContract)
          this.setInitialWorkset(initialWorkset.getContract)
          
          val (delta, nextWorkset) = stepFunction(solutionInput, worksetInput)
          this.setPartialSolutionDelta(delta.getContract)
          this.setNextWorkset(nextWorkset.getContract)
        }
      }
    }
  }
}

