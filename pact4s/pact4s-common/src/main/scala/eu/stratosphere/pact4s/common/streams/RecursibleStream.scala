package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.PactReadWriteSet
import eu.stratosphere.pact4s.common.PactSerializerFactory

trait RecursibleStream[SolutionItem] { this: WrappedDataStream[SolutionItem] =>

  private val initialSolution = this.inner

  def iterate(step: DataStream[SolutionItem] => DataStream[SolutionItem])(implicit serEvS: PactSerializerFactory[SolutionItem], rwEv: PactReadWriteSet) = new KleeneStream(initialSolution, step)

  def untilEmpty[WorksetItem](initialWorkset: DataStream[WorksetItem])(implicit serEvW: PactSerializerFactory[WorksetItem]) = new {

    def iterate(step: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem]))(implicit serEvS: PactSerializerFactory[SolutionItem], rwEv: PactReadWriteSet) = new IncrementalStream(initialSolution, initialWorkset, step)
  }
}

case class KleeneStream[SolutionItem](
  initialSolution: DataStream[SolutionItem],
  step: DataStream[SolutionItem] => DataStream[SolutionItem])
  (implicit serEvS: PactSerializerFactory[SolutionItem], rwEv: PactReadWriteSet)
  extends DataStream[SolutionItem]

case class IncrementalStream[SolutionItem, WorksetItem](
  initialSolution: DataStream[SolutionItem],
  initialWorkset: DataStream[WorksetItem],
  step: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem]))
  (implicit serEvS: PactSerializerFactory[SolutionItem], serEvW: PactSerializerFactory[WorksetItem], rwEv: PactReadWriteSet)
  extends DataStream[SolutionItem]
