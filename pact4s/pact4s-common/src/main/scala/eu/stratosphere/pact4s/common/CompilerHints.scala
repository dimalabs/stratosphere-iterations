package eu.stratosphere.pact4s.common

trait Hintable {

  var hints: Seq[CompilerHint] = null
  def getHints = hints
}

abstract class CompilerHint

case object UniqueKey extends CompilerHint
case class Degree(degreeOfParallelism: Int) extends CompilerHint
case class RecordSize(sizeInBytes: Int) extends CompilerHint
case class ValuesPerKey(numValues: Int) extends CompilerHint
case class Selectivity(selectivityInPercent: Float) extends CompilerHint