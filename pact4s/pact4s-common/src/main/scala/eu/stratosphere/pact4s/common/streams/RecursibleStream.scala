package eu.stratosphere.pact4s.common.streams

trait RecursibleStream[SolutionItem] { self: WrappedDataStream[SolutionItem] =>

  val initialSolution = this.inner

  def iterate(step: DataStream[SolutionItem] => DataStream[SolutionItem]) = new KleeneStream(initialSolution, step)

  def untilEmpty[WorksetItem](initialWorkset: DataStream[WorksetItem]) = new {

    def iterate(step: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) = new IncrementalStream(initialSolution, initialWorkset, step)
  }
}

case class KleeneStream[SolutionItem](
  initialSolution: DataStream[SolutionItem],
  step: DataStream[SolutionItem] => DataStream[SolutionItem])
  extends DataStream[SolutionItem]

case class IncrementalStream[SolutionItem, WorksetItem](
  initialSolution: DataStream[SolutionItem],
  initialWorkset: DataStream[WorksetItem],
  step: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem]))
  extends DataStream[SolutionItem]
