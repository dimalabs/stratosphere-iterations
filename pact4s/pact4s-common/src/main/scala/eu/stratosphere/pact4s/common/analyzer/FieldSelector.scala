package eu.stratosphere.pact4s.common.analyzer

trait FieldSelector[+F <: _ => _] {

  def isGlobalized: Boolean
  def getFields: Array[Int]
  def getGlobalFields: Map[Int, Int] = getFields.zipWithIndex.map(_.swap).filter(_._2 >= 0).toMap

  def markFieldUnused(inputFieldNum: Int)

  def globalize(locations: Map[Int, Int])
  def relocateField(oldPosition: Int, newPosition: Int)
}
