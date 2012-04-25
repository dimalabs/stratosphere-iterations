package eu.stratosphere.pact4s.common.analyzer

trait KeySelector[F <: _ => _] {
  var keyFields: Seq[Int] = null
}

class AnalyzedKeySelector[T1, R] extends KeySelector[T1 => R] {
  keyFields = Seq()
}

