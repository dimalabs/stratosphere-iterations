package eu.stratosphere.pact4s.common

package object analyzer {

  class UDTAnalysisFailedException extends RuntimeException("UDT analysis failed. This should never happen.")
  class UDFAnalysisFailedException extends RuntimeException("UDF analysis failed. This should never happen.")
  class KeySelectorAnalysisFailedException extends RuntimeException("Key selector analysis failed. This should never happen.")

  implicit def unanalyzedUDT[T]: UDT[T] = throw new UDTAnalysisFailedException
  implicit def unanalyzedKeySelector[T1, R]: KeySelector[T1 => R] = throw new KeySelectorAnalysisFailedException
  implicit def unanalyzedUDF1[T1, R]: UDF1[T1 => R] = throw new UDFAnalysisFailedException
  implicit def unanalyzedUDF2[T1, T2, R]: UDF2[(T1, T2) => R] = throw new UDFAnalysisFailedException
}