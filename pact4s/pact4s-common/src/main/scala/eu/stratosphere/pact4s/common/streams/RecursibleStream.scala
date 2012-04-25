package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._

trait RecursibleStream[SolutionItem] { this: WrappedDataStream[SolutionItem] =>

  private val initialSolution = this.inner

  def iterate(step: DataStream[SolutionItem] => DataStream[SolutionItem]) = new KleeneStream(initialSolution, step)

  def untilEmpty[WorksetItem: UDT](initialWorkset: DataStream[WorksetItem]) = new {

    def iterate(step: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) = new IncrementalStream(initialSolution, initialWorkset, step)
  }
}

case class KleeneStream[SolutionItem: UDT](
  initialSolution: DataStream[SolutionItem],
  step: DataStream[SolutionItem] => DataStream[SolutionItem])
  extends DataStream[SolutionItem] {
  
  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}

case class IncrementalStream[SolutionItem: UDT, WorksetItem: UDT](
  initialSolution: DataStream[SolutionItem],
  initialWorkset: DataStream[WorksetItem],
  step: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem]))
  extends DataStream[SolutionItem] {
  
  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}
