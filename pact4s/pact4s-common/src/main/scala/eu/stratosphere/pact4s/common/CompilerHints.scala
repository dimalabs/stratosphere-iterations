package eu.stratosphere.pact4s.common

import eu.stratosphere.pact.common.contract.Contract

trait Hintable {

  var hints: Seq[CompilerHint] = null

  def getHints = hints

  def getHint[T <: CompilerHint : Manifest] = getHints find {
    hint => manifest[T].erasure.isAssignableFrom(hint.getClass)
  }

  def getPactName = getHint[PactName] map { case PactName(pactName) => pactName }

  def applyHints(contract: Contract) = {
    for (hint <- getHints) hint.applyToContract(contract)
  }
}

abstract class CompilerHint {
  def applyToContract(contract: Contract)
}

object CompilerHint {
  implicit def hint2SeqHint(h: CompilerHint): Seq[CompilerHint] = Seq(h)
}

case class Degree(degreeOfParallelism: Int) extends CompilerHint {
  override def applyToContract(contract: Contract) = contract.setDegreeOfParallelism(degreeOfParallelism)
}

case class RecordSize(sizeInBytes: Float) extends CompilerHint {
  override def applyToContract(contract: Contract) = contract.getCompilerHints().setAvgBytesPerRecord(sizeInBytes)
}

case class Selectivity(selectivityInPercent: Float) extends CompilerHint {
  override def applyToContract(contract: Contract) = contract.getCompilerHints().setAvgRecordsEmittedPerStubCall(selectivityInPercent)
}

case class PactName(pactName: String) extends CompilerHint {
  override def applyToContract(contract: Contract) = {}
}