package eu.stratosphere.pact4s.common.analyzer

trait UDF1Builder[T1, R] {
  type UDF[_] = UDF1[T1 => R]
}

trait UDF1[F <: _ => _] {
  
  var readFields: Array[Int] = null
  var writeFields: Array[Int] = null
  var copyFields: Map[Int, Int] = null
}

class AnalyzedUDF1[T1, R](inputCount: Int, outputCount: Int) extends UDF1[T1 => R] {
  readFields = (0 until inputCount).toArray
  writeFields = (0 until outputCount).toArray
  copyFields = Map()
}

trait UDF2Builder[T1, T2, R] {
  type UDF[_] = UDF2[(T1, T2) => R]
}

trait UDF2[F <: (_, _) => _] {
  var leftReadFields: Array[Int] = null
  var leftCopyFields: Map[Int, Int] = null

  var rightReadFields: Array[Int] = null
  var rightCopyFields: Map[Int, Int] = null

  var writeFields: Array[Int] = null
}

class AnalyzedUDF2[T1, T2, R](leftInputCount: Int, rightInputCount: Int, outputCount: Int) extends UDF2[(T1, T2) => R] {
  leftReadFields = (0 until leftInputCount).toArray
  leftCopyFields = Map()

  rightReadFields = (0 until rightInputCount).toArray
  rightCopyFields = Map()

  writeFields = (0 until outputCount).toArray
}

