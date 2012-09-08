/**
 * *********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * ********************************************************************************************************************
 */

package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analyzer._

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.JavaConversions._
import java.io.ObjectInputStream

import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactList

final class ListUDT[T, L[T] <: GenTraversableOnce[T]](implicit udtInst: UDT[T], bf: CanBuildFrom[_, T, L[T]]) extends UDT[L[T]] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactList[PactRecord]])

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, udtInst)

  private def createSerializer(indexMap: Array[Int], udtInst: UDT[T])(implicit bf: CanBuildFrom[_, T, L[T]]) = new UDTSerializer[L[T]] {

    private val index = indexMap(0)

    @transient private val udt = udtInst
    private var inner: UDTSerializer[T] = null

    override def init() = {
      inner = udt.getSerializerWithDefaultLayout
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => List(index)
      case _     => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(items: L[T], record: PactRecord) = {
      // This method will be reentrant if T contains a List[T]

      if (index >= 0) {
        record.updateBinaryRepresenation()

        val pactField = new PactListImpl
        val it = items.toIterator

        while (it.hasNext) {
          val item = it.next
          if (item != null) {
            val record = new PactRecord()
            inner.serialize(item, record)
            record.updateBinaryRepresenation()
            pactField.add(record)
          } else {
            pactField.add(null)
          }
        }

        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): L[T] = {
      // This method will be reentrant if T contains a List[T]

      if (index >= 0) {
        val pactField = new PactListImpl
        record.getFieldInto(index, pactField)
        val b = bf()
        b.sizeHint(pactField.size())
        for (item <- pactField) b += inner.deserialize(item)
        b.result
      } else {
        bf().result
      }
    }

    private class PactListImpl extends PactList[PactRecord]
  }
}