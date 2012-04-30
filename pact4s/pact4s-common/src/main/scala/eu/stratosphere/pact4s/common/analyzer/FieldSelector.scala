package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

trait FieldSelector[+F <: _ => _] {

  def isGlobalized: Boolean
  def getFields: Array[Int]

  def globalize(inputLocation: Int)
  def relocateField(oldPosition: Int, newPosition: Int)
}
