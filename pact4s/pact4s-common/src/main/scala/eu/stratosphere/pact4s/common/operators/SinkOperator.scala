package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._

class SinkToSourceOperator[In: UDT](sink: DataSink[In]) extends Serializable {

  def <~(source: DataStream[In]) = new PlanOutput(source, sink)
}

class SourceToSinkOperator[In: UDT](source: DataStream[In]) extends Serializable {

  def ~>(sink: DataSink[In]) = new PlanOutput(source, sink)
}