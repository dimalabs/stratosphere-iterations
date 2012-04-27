package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

case class KleeneIterateStream[SolutionItem: UDT](
  initialSolution: DataStream[SolutionItem],
  stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem])
  extends DataStream[SolutionItem] {

  override def contract = throw new UnsupportedOperationException("Not implemented yet")
}

case class IncrementalIterateStream[SolutionItem: UDT, WorksetItem: UDT](
  initialSolution: DataStream[SolutionItem],
  initialWorkset: DataStream[WorksetItem],
  stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem]))
  extends DataStream[SolutionItem] {

  override def contract = throw new UnsupportedOperationException("Not implemented yet")
}
