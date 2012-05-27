package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactDouble

final class DoubleUDT extends UDT[Double] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactDouble])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Double] {

    private val index = indexMap(0)

    @transient private var pactField = new PactDouble()

    override def serialize(item: Double, record: PactRecord) = {
      if (index >= 0) {
        pactField.setValue(item)
        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): Double = {
      if (index >= 0) {
        record.getFieldInto(index, pactField)
        pactField.getValue()
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