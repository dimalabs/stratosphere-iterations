package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._

trait SinkToSourceOperator[In] { this: WrappedDataSink[In] =>

  private val sink = this.inner

  def <~(source: DataStream[In]) = new PlanOutput(source, sink)
}

trait SourceToSinkOperator[In] { this: WrappedDataStream[In] =>

  private val source = this.inner

  def ~>(sink: DataSink[In]) = new PlanOutput(source, sink)
}