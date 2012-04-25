package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.PlanOutput
import eu.stratosphere.pact4s.common.streams.DataSink

trait SourceToSinkOperator[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def ~>[S](sink: DataSink[In, S]) = new PlanOutput(input, sink)
}