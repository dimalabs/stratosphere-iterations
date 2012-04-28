package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

trait KeySelector[+F <: _ => _] {

  val keyFieldTypes: Array[Class[_ <: PactKey]]

  def isGlobalized: Boolean
  def getKeyFields: Array[Int]

  def globalize(inputLocation: Int)
  def relocateKeyField(oldPosition: Int, newPosition: Int)
}
