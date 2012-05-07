package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._

class SinkToSourceOperator[In: UDT](sink: DataSink[In]) {

  def <~(source: DataStream[In]) = new PlanOutput(source, sink)
}

class SourceToSinkOperator[In: UDT](source: DataStream[In]) {

  def ~>(sink: DataSink[In]) = new PlanOutput(source, sink)
}