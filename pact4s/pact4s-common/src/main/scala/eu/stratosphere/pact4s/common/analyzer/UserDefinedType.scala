package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact.common.`type`.PactRecord

trait UDT[T] {

  val fieldCount: Int

  def createSerializer(indexMap: Array[Int]): UDTSerializer[T]
}

abstract class UDTSerializer[T] {

  def serialize(item: T, record: PactRecord)
  def deserialize(record: PactRecord): T
}

object UDTSerializer {
  implicit def toAny(s: UDTSerializer[_]): UDTSerializer[Any] = s.asInstanceOf[UDTSerializer[Any]]
}