package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.streams.PlanOutput

abstract class PactProgram {

  def defaultParallelism = 1
  def outputs: Seq[PlanOutput[_]]
}

