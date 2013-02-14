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

package eu.stratosphere.pact4s.common.operators

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class UnionOperator[T: UDT](firstInput: DataStream[T]) extends Serializable {

  def union(secondInput: DataStream[T]) = new DataStream[T] with Hintable {

    override def createContract = {

      val firstInputs = firstInput.getContract match {
        case Union4sContract(inputs) => inputs.toList
        case c                       => List(c)
      }

      val secondInputs = secondInput.getContract match {
        case Union4sContract(inputs) => inputs.toList
        case c                       => List(c)
      }

      val builder = Union4sContract.newBuilder.inputs(firstInputs ++ secondInputs)

      new MapContract(builder) with Union4sContract[T] {

        // Union is a no-op placeholder that reads nothing and writes nothing.
        // Its udf's outputFields specifies the write location for the union's 
        // children and the read location for its parent.
        override val udf = new UDF0[T]

        override def persistHints() = applyHints(this)
      }
    }
  }
}