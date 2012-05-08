package eu.stratosphere.pact4s.common

abstract class PactProgram extends Serializable {

  def defaultParallelism = 1
  def outputs: Seq[PlanOutput[_]]
}

