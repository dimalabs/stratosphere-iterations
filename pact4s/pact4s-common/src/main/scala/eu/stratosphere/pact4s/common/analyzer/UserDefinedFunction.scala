package eu.stratosphere.pact4s.common.analyzer

abstract sealed class AmbientFieldBehavior

object AmbientFieldBehavior {
  case object Default extends AmbientFieldBehavior
  case object Forward extends AmbientFieldBehavior
  case object Discard extends AmbientFieldBehavior
}

trait UDF extends Serializable {

  def isGlobalized: Boolean
  def getWriteFields: Array[Int]

  def relocateInputField(oldPosition: Int, newPosition: Int)
}

trait UDF1[+F <: _ => _] extends UDF {

  def getReadFields: Array[Int]
  def getCopiedFields: Map[Int, Int]
  def getForwardedFields: Iterable[Int]
  def getDiscardedFields: Iterable[Int]

  def markInputFieldUnread(inputFieldNum: Int)
  def markInputFieldCopied(fromInputFieldNum: Int, toOutputFieldNum: Int)

  def globalize(inputLocation: Int, outputLocation: Int)
  def setAmbientFieldBehavior(position: Int, behavior: AmbientFieldBehavior)
}

trait UDF2[+F <: (_, _) => _] extends UDF {

  def getReadFields: (Array[Int], Array[Int])
  def getCopiedFields: Map[Int, Either[Int, Int]]
  def getForwardedFields: (Iterable[Int], Iterable[Int])
  def getDiscardedFields: (Iterable[Int], Iterable[Int])

  def markInputFieldUnread(inputFieldNum: Either[Int, Int])
  def markInputFieldCopied(fromInputFieldNum: Either[Int, Int], toOutputFieldNum: Int)

  def globalize(leftInputLocation: Int, rightInputLocation: Int, outputLocation: Int)
  def setAmbientFieldBehavior(position: Either[Int, Int], behavior: AmbientFieldBehavior)
}

