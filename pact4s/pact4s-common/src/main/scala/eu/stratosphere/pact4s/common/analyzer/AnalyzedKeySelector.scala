package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

class AnalyzedKeySelector[T1, R](keyFieldCount: Int) extends KeySelector[T1 => R] {

  private var globalized = false
  private val keyFields = (0 until keyFieldCount).toArray

  override val keyFieldTypes = new Array[Class[_ <: PactKey]](keyFieldCount)

  override def isGlobalized = globalized
  override def getKeyFields = keyFields

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != globalized)
      throw new IllegalStateException()
  }

  override def globalize(inputLocation: Int) = {
    assertGlobalized(false)

    for (fieldNum <- 0 to keyFields.length if keyFields(fieldNum) > -1) {
      keyFields(fieldNum) += inputLocation
    }

    globalized = true
  }

  override def relocateKeyField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    for (fieldNum <- 0 to keyFields.length if keyFields(fieldNum) == oldPosition) {
      keyFields(fieldNum) = newPosition
    }
  }
}

