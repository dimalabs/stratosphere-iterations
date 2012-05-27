package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactInteger

final class CharUDT extends UDT[Char] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactInteger])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Char] {

    private val index = indexMap(0)

    @transient private var pactField = new PactInteger()

    override def serialize(item: Char, record: PactRecord) = {
      if (index >= 0) {
        pactField.setValue(item)
        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): Char = {
      if (index >= 0) {
        record.getFieldInto(index, pactField)
        pactField.getValue().toChar
      } else {
        '\0'
      }
    }

    private def readObject(in: ObjectInputStream) = {
      in.defaultReadObject()
      pactField = new PactInteger()
    }
  }
}