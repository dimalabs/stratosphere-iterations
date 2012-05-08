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
  def getOutputLocation: Int
  def getOutputFields: Map[Int, Int]

  def relocateInputField(oldPosition: Int, newPosition: Int)
}

trait UDF1[+F <: _ => _] extends UDF {

  def getReadFields: Array[Int]
  def getCopiedFields: Map[Int, Int]
  def getForwardedFields: Array[Int]
  def getDiscardedFields: Array[Int]

  def copy(): UDF1[F]
  def markInputFieldUnread(inputFieldNum: Int)
  def markInputFieldCopied(fromInputFieldNum: Int, toOutputFieldNum: Int)

  def globalize(inputLocations: Map[Int, Int], outputLocation: Int): Int
  def globalizeInPlace(inputLocations: Map[Int, Int])
  def setAmbientFieldBehavior(position: Int, behavior: AmbientFieldBehavior)
}

trait UDF2[+F <: (_, _) => _] extends UDF {

  def getReadFields: (Array[Int], Array[Int])
  def getCopiedFields: Map[Int, Either[Int, Int]]
  def getForwardedFields: (Array[Int], Array[Int])
  def getDiscardedFields: (Array[Int], Array[Int])

  def getAllForwardedFields = {
    val (left, right) = getForwardedFields
    (left ++ right) toArray
  }

  def getAllDiscardedFields = {
    val (left, right) = getDiscardedFields
    (left ++ right) toArray
  }

  def markInputFieldUnread(inputFieldNum: Either[Int, Int])
  def markInputFieldCopied(fromInputFieldNum: Either[Int, Int], toOutputFieldNum: Int)

  def globalize(leftInputLocations: Map[Int, Int], rightInputLocations: Map[Int, Int], outputLocation: Int): Int
  def setAmbientFieldBehavior(position: Either[Int, Int], behavior: AmbientFieldBehavior)
}

