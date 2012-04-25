package eu.stratosphere.pact4s.common.analyzer

trait KeyBuilder[T1, R] {
  type Selector[_] = KeySelector[T1 => R]
}

trait KeySelector[F <: _ => _] {
  var keyFields: Seq[Int] = null
}

class AnalyzedKeySelector[T1, R] extends KeySelector[T1 => R] {
  keyFields = Seq()
}

