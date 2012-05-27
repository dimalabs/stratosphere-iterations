package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactString

final class StringUDT extends UDT[String] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactString])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[String] {

    private val index = indexMap(0)

    @transient private var pactField = new PactString()

    override def serialize(item: String, record: PactRecord) = {
      if (index >= 0) {
        pactField.setValue(item)
        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): String = {
      if (index >= 0) {
        record.getFieldInto(index, pactField)
        pactField.getValue()
      } else {
        null
      }
    }

    private def readObject(in: ObjectInputStream) = {
      in.defaultReadObject()
      pactField = new PactString()
    }
  }
}