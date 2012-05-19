package eu.stratosphere.pact4s.common

abstract class PactProgram extends Serializable {

  def outputs: Seq[PlanOutput[_]]
}

