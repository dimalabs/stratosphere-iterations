package eu.stratosphere.pact4s.common

class PactReadWriteSet {

  var readFields: Set[Int] = Set()
  var writeFields: Set[Int] = Set()
  var copyFields: Map[Int, Int] = Map()
}

object PactReadWriteSet {
  
  implicit val allFields = new PactReadWriteSet {
    readFields = null
    writeFields = null
    copyFields = null
  }
}