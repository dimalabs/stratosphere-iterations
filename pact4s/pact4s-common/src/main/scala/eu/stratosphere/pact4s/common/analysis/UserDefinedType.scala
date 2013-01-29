/**
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
 */

package eu.stratosphere.pact4s.common.analysis

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom

import eu.stratosphere.pact.common.`type`.{ Key => PactKey }
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactList
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.pact.common.`type`.PactRecord

trait UDT[T] extends Serializable {

  val fieldTypes: Array[Class[_ <: PactValue]]
  def numFields = fieldTypes.length

  def getFieldIndexes(selections: Seq[Seq[String]]): List[Int] = {
    val ser = getSerializerWithDefaultLayout
    selections.toList flatMap ser.getFieldIndex
  }

  def getKeySet(fields: Seq[Int]): Array[Class[_ <: PactKey]] = {
    fields map { fieldNum => fieldTypes(fieldNum).asInstanceOf[Class[_ <: PactKey]] } toArray
  }

  def getSerializer(indexMap: Array[Int]): UDTSerializer[T] = {
    val ser = createSerializer(indexMap)
    ser.init()
    ser
  }

  @transient private var defaultSerializer: UDTSerializer[T] = null

  def getSerializerWithDefaultLayout: UDTSerializer[T] = {
    // This method will be reentrant if T is a recursive type
    if (defaultSerializer == null) {
      defaultSerializer = createSerializer((0 until numFields) toArray)
      defaultSerializer.init()
    }
    defaultSerializer
  }

  protected def createSerializer(indexMap: Array[Int]): UDTSerializer[T]
}

abstract class UDTSerializer[T] extends Serializable {

  protected[analysis] def init(): Unit = ()

  protected def invalidSelection(selection: Seq[String]) = {
    val sel = selection match {
      case null  => "<null>"
      case Seq() => "<all>"
      case _     => selection.mkString(".")
    }
    throw new UDT.UDTSelectionFailedException("Invalid selection: " + sel)
  }

  def getFieldIndex(selection: Seq[String]): List[Int]
  def serialize(item: T, record: PactRecord)
  def deserializeRecyclingOff(record: PactRecord): T
  def deserializeRecyclingOn(record: PactRecord): T
}

trait UDTLowPriorityImplicits {

  class UDTAnalysisFailedException extends RuntimeException("UDT analysis failed. This should have been caught at compile time.")
  class UDTSelectionFailedException(msg: String) extends RuntimeException(msg)

  implicit def unanalyzedUDT[T]: UDT[T] = throw new UDTAnalysisFailedException
}

object UDT extends UDTLowPriorityImplicits {

  // UDTs needed by library code

  object NothingUDT extends UDT[Nothing] {
    override val fieldTypes = Array[Class[_ <: PactValue]]()
    override def createSerializer(indexMap: Array[Int]) = throw new UnsupportedOperationException("Cannot create UDTSerializer for type Nothing")
  }

  object StringUDT extends UDT[String] {

    override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactString])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[String] {

      private val index = indexMap(0)

      @transient private var pactField = new PactString()

      override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
        case Seq() => List(index)
        case _     => invalidSelection(selection)
      }

      override def serialize(item: String, record: PactRecord) = {
        if (index >= 0) {
          pactField.setValue(item)
          record.setField(index, pactField)
        }
      }

      override def deserializeRecyclingOff(record: PactRecord): String = {
        if (index >= 0) {
          record.getFieldInto(index, pactField)
          pactField.getValue()
        } else {
          null
        }
      }

      override def deserializeRecyclingOn(record: PactRecord): String = {
        if (index >= 0) {
          record.getFieldInto(index, pactField)
          pactField.getValue()
        } else {
          null
        }
      }

      private def readObject(in: java.io.ObjectInputStream) = {
        in.defaultReadObject()
        pactField = new PactString()
      }
    }
  }
}

