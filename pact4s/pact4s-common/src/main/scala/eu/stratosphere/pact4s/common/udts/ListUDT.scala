package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.JavaConversions._
import java.io.ObjectInputStream

import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactList

final class ListUDT[T, L[T] <: GenTraversableOnce[T]](implicit udt: UDT[T], bf: CanBuildFrom[GenTraversableOnce[T], T, L[T]]) extends UDT[L[T]] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactList[PactRecord]])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[L[T]] {

    private val index = indexMap(0)
    private val inner = udt.createSerializer((0 until udt.numFields) toArray)

    @transient private var pactField = new PactList[PactRecord]() {}

    override def serialize(items: L[T], record: PactRecord) = {
      if (index >= 0) {
        pactField.clear()

        items.foreach { item =>
          val record = new PactRecord()
          inner.serialize(item, record)
          record.updateBinaryRepresenation()
          pactField.add(record)
        }

        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): L[T] = {

      if (index >= 0) {
        record.getFieldInto(index, pactField)
        val items = pactField map { inner.deserialize(_) }
        val b = bf(items)
        b.sizeHint(pactField.size())
        for (item <- items) b += item
        b.result
      } else {
        bf().result
      }
    }

    private def readObject(in: ObjectInputStream) = {
      in.defaultReadObject()
      pactField = new PactList[PactRecord]() {}
    }
  }
}