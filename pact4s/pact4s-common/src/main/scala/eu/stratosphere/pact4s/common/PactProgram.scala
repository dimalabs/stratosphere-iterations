package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analyzer._

abstract class PactProgram {

  type DataStream[T] = eu.stratosphere.pact4s.common.streams.DataStream[T]

  def name: String
  def description: String
  def defaultParallelism = 1
  def outputs: Seq[PlanOutput]

  case class PlanOutput(source: DataStream[_], sink: DataSink[_, _])

  case class PlanOutputFromSource[T](source: DataStream[T]) {
    def ~>[S](sink: DataSink[T, S]) = new PlanOutput(source, sink)
  }

  case class PlanOutputFromSink[T, S](sink: DataSink[T, S]) {
    def <~(source: DataStream[T]) = new PlanOutput(source, sink)
  }

  implicit def stream2PlanOutputBuilder[T](source: DataStream[T]): PlanOutputFromSource[T] = new PlanOutputFromSource(source)
  implicit def sink2PlanOutputBuilder[T, S](sink: DataSink[T, S]): PlanOutputFromSink[T, S] = new PlanOutputFromSink(sink)

  implicit def planOutput2Seq(p: PlanOutput): Seq[PlanOutput] = Seq(p)

  case class DataSource[S, T: UDT, F: UDF1Builder[S, T]#UDF](url: String, parser: S => T) extends DataStream[T] {
    val udf = implicitly[UDF1[S => T]]
    def getContract = throw new UnsupportedOperationException("Not implemented yet")
  }
  
  case class DataSink[T, S](url: String, formatter: T => S) extends Hintable
}

