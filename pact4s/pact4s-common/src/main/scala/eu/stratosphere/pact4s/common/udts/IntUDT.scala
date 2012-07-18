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

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactInteger

final class IntUDT extends UDT[Int] {

  override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactInteger])

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Int] {

    private val index = indexMap(0)

    @transient private var pactField = new PactInteger()

    override def serialize(item: Int, record: PactRecord) = {
      if (index >= 0) {
        pactField.setValue(item)
        record.setField(index, pactField)
      }
    }

    override def deserialize(record: PactRecord): Int = {
      if (index >= 0) {
        record.getFieldInto(index, pactField)
        pactField.getValue()
      } else {
        0
      }
    }

    private def readObject(in: ObjectInputStream) = {
      in.defaultReadObject()
      pactField = new PactInteger()
    }
  }
}