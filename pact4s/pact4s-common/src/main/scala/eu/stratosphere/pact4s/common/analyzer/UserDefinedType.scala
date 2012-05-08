package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact.common.`type`.{ Key => PactKey }
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.PactRecord

trait UDT[T] extends Serializable {

  val fieldTypes: Array[Class[_ <: PactValue]]
  def numFields = fieldTypes.length

  def getKeySet(fields: Seq[Int]): Array[Class[_ <: PactKey]] = {
    fields map { fieldNum => fieldTypes(fieldNum).asInstanceOf[Class[_ <: PactKey]] } toArray
  }

  def createSerializer(indexMap: Array[Int]): UDTSerializer[T]
}

abstract class UDTSerializer[T] extends Serializable {

  def serialize(item: T, record: PactRecord)
  def deserialize(record: PactRecord): T
}
