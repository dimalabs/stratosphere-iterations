package eu.stratosphere.pact4s.common

package object analyzer {

  type UDF1Builder[T1, R] = { type UDF[_] = UDF1[T1 => R] }
  type UDF2Builder[T1, T2, R] = { type UDF[_] = UDF2[(T1, T2) => R] }
  type SelectorBuilder[T1, R] = { type Selector[_] = FieldSelector[T1 => R] }

  class UDTAnalysisFailedException extends RuntimeException("UDT analysis failed. This should never happen.")
  class UDFAnalysisFailedException extends RuntimeException("UDF analysis failed. This should never happen.")
  class FieldSelectorAnalysisFailedException extends RuntimeException("Field selector analysis failed. This should never happen.")

  implicit def unanalyzedUDT[T]: UDT[T] = throw new UDTAnalysisFailedException
  implicit def unanalyzedUDF1[T1, R]: UDF1[T1 => R] = throw new UDFAnalysisFailedException
  implicit def unanalyzedUDF2[T1, T2, R]: UDF2[(T1, T2) => R] = throw new UDFAnalysisFailedException
  implicit def unanalyzedFieldSelector[T1, R]: FieldSelector[T1 => R] = throw new FieldSelectorAnalysisFailedException
}