package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.PlanOutput
import eu.stratosphere.pact4s.common.streams.DataStream

trait SinkToSourceOperator[In, Out] { this: WrappedDataSink[In, Out] =>

  private val sink = this.inner

  def <~(source: DataStream[In]) = new PlanOutput(source, sink)
}