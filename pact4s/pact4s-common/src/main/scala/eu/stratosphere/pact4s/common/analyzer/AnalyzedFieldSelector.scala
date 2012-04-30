package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

class AnalyzedFieldSelector[T1, R](fieldCount: Int) extends FieldSelector[T1 => R] {

  private var globalized = false
  private val fields = (0 until fieldCount).toArray

  override def isGlobalized = globalized
  override def getFields = fields

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != globalized)
      throw new IllegalStateException()
  }

  override def globalize(inputLocation: Int) = {
    assertGlobalized(false)

    for (fieldNum <- 0 to fields.length if fields(fieldNum) > -1) {
      fields(fieldNum) += inputLocation
    }

    globalized = true
  }

  override def relocateField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    for (fieldNum <- 0 to fields.length if fields(fieldNum) == oldPosition) {
      fields(fieldNum) = newPosition
    }
  }
}

