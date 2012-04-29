package eu.stratosphere.pact4s.common

abstract class PactProgram {

  type DataStream[T] = eu.stratosphere.pact4s.common.streams.DataStream[T]
  type DataSink[In, T] = eu.stratosphere.pact4s.common.streams.DataSink[In, T]
  type DataSource[S, T, F] = eu.stratosphere.pact4s.common.streams.DataSource[S, T, F]
  type PlanOutput[T] = eu.stratosphere.pact4s.common.streams.PlanOutput[T]

  def defaultParallelism = 1
  def outputs: Seq[PlanOutput[_]]
}

