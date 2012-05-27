package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactDouble

final class FloatUDT extends UDT[Float] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactDouble])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Float] {

    private val index = indexMap(0)

    @transient private var pactField = new PactDouble()

    override def serialize(item: Float, record: PactRecord) = {
      if (index >= 0) {
        pactField.setValue(item)
        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): Float = {
      if (index >= 0) {
        record.getFieldInto(index, pactField)
        pactField.getValue().toFloat
      } else {
        0
      }
    }

    private def readObject(in: ObjectInputStream) = {
      in.defaultReadObject()
      pactField = new PactDouble()
    }
  }
}