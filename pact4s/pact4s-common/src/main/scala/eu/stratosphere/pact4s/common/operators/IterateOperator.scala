package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait IterateOperator[SolutionItem] { this: WrappedDataStream[SolutionItem] =>

  private val initialSolution = this.inner

  def keyBy[Key, SolutionKeySelector: SelectorBuilder[SolutionItem, Key]#Selector](keySelector: SolutionItem => Key) = new {

    def iterate(stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]): DataStream[SolutionItem] = new DataStream[SolutionItem] {

      override def contract = throw new UnsupportedOperationException("Not implemented yet")
    }

    def untilEmpty[WorksetItem: UDT](initialWorkset: DataStream[WorksetItem]) = new {

      def iterate(stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])): DataStream[SolutionItem] = new DataStream[SolutionItem] {

        override def contract = throw new UnsupportedOperationException("Not implemented yet")
      }
    }
  }
}

