package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait IterateOperator[SolutionItem] { this: WrappedDataStream[SolutionItem] =>

  private val initialSolution = this.inner

  def iterate(stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]) = new KleeneIterateStream(initialSolution, stepFunction)

  def untilEmpty[WorksetItem: UDT](initialWorkset: DataStream[WorksetItem]) = new {

    def iterate(stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) = new IncrementalIterateStream(initialSolution, initialWorkset, stepFunction)
  }
}

