package eu.stratosphere.pact4s.common.analyzer

class AnalyzedFieldSelector[T1, R](fieldCount: Int) extends FieldSelector[T1 => R] {

  private var globalized = false
  private val fields = (0 until fieldCount).toArray

  override def isGlobalized = globalized
  override def getFields = fields

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != globalized)
      throw new IllegalStateException()
  }

  override def markFieldUnused(inputFieldNum: Int) = {
    assertGlobalized(false)
    fields(inputFieldNum) = -1
  }

  override def globalize(locations: Map[Int, Int]) = {

    if (!globalized) {

      for (fieldNum <- 0 until fields.length if fields(fieldNum) >= 0) {
        fields(fieldNum) = locations(fieldNum)
      }

      globalized = true
    }
  }

  override def relocateField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    for (fieldNum <- 0 until fields.length if fields(fieldNum) == oldPosition) {
      fields(fieldNum) = newPosition
    }
  }
}

