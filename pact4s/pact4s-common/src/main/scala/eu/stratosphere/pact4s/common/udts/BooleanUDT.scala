package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactInteger

final class BooleanUDT extends UDT[Boolean] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactInteger])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Boolean] {

    private val index = indexMap(0)

    @transient private var pactField = new PactInteger()

    override def serialize(item: Boolean, record: PactRecord) = {
      if (index >= 0) {
        pactField.setValue(if (item) 1 else 0)
        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): Boolean = {
      if (index >= 0) {
        record.getFieldInto(index, pactField)
        pactField.getValue() > 0
      } else {
        false
      }
    }

    private def readObject(in: ObjectInputStream) = {
      in.defaultReadObject()
      pactField = new PactInteger()
    }
  }
}