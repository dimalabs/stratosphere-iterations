package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analyzer._

abstract class PactProgram {

  type DataStream[T] = eu.stratosphere.pact4s.common.streams.DataStream[T]
  type DataSink[T, S] = eu.stratosphere.pact4s.common.streams.DataSink[T, S]
  type DataSource[S, T, F] = eu.stratosphere.pact4s.common.streams.DataSource[S, T, F]

  def name: String
  def description: String
  def defaultParallelism = 1
  def outputs: Seq[PlanOutput]
}

