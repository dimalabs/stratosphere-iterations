package eu.stratosphere.pact4s.common.analyzer

trait KeySelector[+F <: _ => _] {
  var readFields: Array[Int] = null
  var writeFields: Array[Int] = null
  var copyFields: Map[Int, Int] = null
}

class AnalyzedKeySelector[T1, R](inputCount: Int, keyFieldCount: Int) extends KeySelector[T1 => R] {
  readFields = (0 until inputCount).toArray
  writeFields = (0 until keyFieldCount).toArray
  copyFields = Map()
}

