package eu.stratosphere.pact4s.common.operators

import java.io.OutputStream

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait SourceToSinkOperator[In] { this: WrappedDataStream[In] =>

  private val source = this.inner

  def ~>(sink: DataSink[In, _]) = new PlanOutput(source, sink)
}