package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactLong

final class LongUDT extends UDT[Long] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactLong])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Long] {

    private val index = indexMap(0)

    @transient private var pactField = new PactLong()

    override def serialize(item: Long, record: PactRecord) = {
      if (index >= 0) {
        pactField.setValue(item)
        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): Long = {
      if (index >= 0) {
        record.getFieldInto(index, pactField)
        pactField.getValue()
      } else {
        0
      }
    }

    private def readObject(in: ObjectInputStream) = {
      in.defaultReadObject()
      pactField = new PactLong()
    }
  }
}