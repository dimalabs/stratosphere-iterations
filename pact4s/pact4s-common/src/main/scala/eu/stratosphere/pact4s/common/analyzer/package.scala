package eu.stratosphere.pact4s.common

package object analyzer {

  type UDF1Builder[T1, R] = { type UDF[_] = UDF1[T1 => R] }
  type UDF2Builder[T1, T2, R] = { type UDF[_] = UDF2[(T1, T2) => R] }
  type SelectorBuilder[T1, R] = { type Selector[_] = FieldSelector[T1 => R] }

  class UDTAnalysisFailedException extends RuntimeException("UDT analysis failed. This should never happen.")
  class UDFAnalysisFailedException extends RuntimeException("UDF analysis failed. This should never happen.")
  class FieldSelectorAnalysisFailedException extends RuntimeException("Field selector analysis failed. This should never happen.")

  //implicit def unanalyzedUDT[T]: UDT[T] = throw new UDTAnalysisFailedException
  //implicit def unanalyzedUDF1[T1, R]: UDF1[T1 => R] = throw new UDFAnalysisFailedException
  //implicit def unanalyzedUDF2[T1, T2, R]: UDF2[(T1, T2) => R] = throw new UDFAnalysisFailedException
  //implicit def unanalyzedFieldSelector[T1, R]: FieldSelector[T1 => R] = throw new FieldSelectorAnalysisFailedException

  def defaultUDF1[T1: UDT, R: UDT]: UDF1[T1 => R] = new AnalyzedUDF1[T1, R](implicitly[UDT[T1]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF1IterR[T1: UDT, R: UDT]: UDF1[T1 => Iterator[R]] = new AnalyzedUDF1[T1, Iterator[R]](implicitly[UDT[T1]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF1IterT[T1: UDT, R: UDT]: UDF1[Iterator[T1] => R] = new AnalyzedUDF1[Iterator[T1], R](implicitly[UDT[T1]].numFields, implicitly[UDT[R]].numFields)

  def defaultUDF2[T1: UDT, T2: UDT, R: UDT]: UDF2[(T1, T2) => R] = new AnalyzedUDF2[T1, T2, R](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF2IterR[T1: UDT, T2: UDT, R: UDT]: UDF2[(T1, T2) => Iterator[R]] = new AnalyzedUDF2[T1, T2, Iterator[R]](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF2IterT[T1: UDT, T2: UDT, R: UDT]: UDF2[(Iterator[T1], Iterator[T2]) => R] = new AnalyzedUDF2[Iterator[T1], Iterator[T2], R](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF2IterTR[T1: UDT, T2: UDT, R: UDT]: UDF2[(Iterator[T1], Iterator[T2]) => Iterator[R]] = new AnalyzedUDF2[Iterator[T1], Iterator[T2], Iterator[R]](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)

  def defaultFieldSelectorT[T1: UDT, R]: FieldSelector[T1 => R] = new AnalyzedFieldSelector[T1, R](implicitly[UDT[T1]].numFields)
  def defaultFieldSelectorR[T1, R: UDT]: FieldSelector[T1 => R] = new AnalyzedFieldSelector[T1, R](implicitly[UDT[R]].numFields)

  def getFieldSelector[T1: UDT, R](selFields: Int*): FieldSelector[T1 => R] = new AnalyzedFieldSelector[T1, R](implicitly[UDT[T1]].numFields) {
    for (field <- 0 until implicitly[UDT[T1]].numFields if !selFields.contains(field))
      markFieldUnused(field)
  }

  implicit object StringUDT extends UDT[String] {

    import eu.stratosphere.pact.common.`type`.PactRecord
    import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
    import eu.stratosphere.pact.common.`type`.base.PactString

    override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactString])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[String] {

      private val ix0 = indexMap(0)

      private val w0 = new PactString()

      override def serialize(item: String, record: PactRecord) = {
        val v0 = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }
      }

      override def deserialize(record: PactRecord): String = {
        var v0: String = null

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        v0
      }
    }
  }
}