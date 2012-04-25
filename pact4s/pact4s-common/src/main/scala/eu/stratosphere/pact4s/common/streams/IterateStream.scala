package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._

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
