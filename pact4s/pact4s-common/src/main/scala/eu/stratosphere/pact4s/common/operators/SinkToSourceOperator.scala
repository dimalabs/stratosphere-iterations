package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait SinkToSourceOperator[In] { this: WrappedDataSink[In] =>

  private val sink = this.inner

  def <~(source: DataStream[In]) = new PlanOutput(source, sink)
}