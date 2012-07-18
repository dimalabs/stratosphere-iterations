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

package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class CoGroupOperator[LeftIn: UDT](leftInput: DataStream[LeftIn]) extends Serializable {

  def cogroup[RightIn: UDT](rightInput: DataStream[RightIn]) = new Serializable {

    def on[Key, LeftKeySelector: SelectorBuilder[LeftIn, Key]#Selector](leftKeySelector: LeftIn => Key) = new Serializable {

      def isEqualTo[RightKeySelector: SelectorBuilder[RightIn, Key]#Selector](rightKeySelector: RightIn => Key) = new Serializable {

        def map[Out: UDT, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Out]#UDF](mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out) = createStream(Left(mapFunction))

        def flatMap[Out: UDT, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Iterator[Out]]#UDF](mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]) = createStream(Right(mapFunction))

        private def createStream[Out: UDT, R, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], R]#UDF](
          mapFunction: Either[(Iterator[LeftIn], Iterator[RightIn]) => Out, (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]): DataStream[Out] = new DataStream[Out] {

          override def createContract = {

            val leftKey = implicitly[FieldSelector[LeftIn => Key]]
            val rightKey = implicitly[FieldSelector[RightIn => Key]]
            val leftKeyFields = leftKey.getFields filter { _ >= 0 }
            val rightKeyFields = rightKey.getFields filter { _ >= 0 }
            val keyFieldTypes = implicitly[UDT[LeftIn]].getKeySet(leftKeyFields)

            new CoGroupContract(CoGroup4sContract.getStub, keyFieldTypes, leftKeyFields, rightKeyFields, leftInput.getContract, rightInput.getContract) with CoGroup4sContract[Key, LeftIn, RightIn, Out] {

              override val leftKeySelector = leftKey
              override val rightKeySelector = rightKey
              override val leftUDT = implicitly[UDT[LeftIn]]
              override val rightUDT = implicitly[UDT[RightIn]]
              override val outputUDT = implicitly[UDT[Out]]
              override val coGroupUDF = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => R]]
              override val userFunction = mapFunction
            }
          }
        }
      }
    }
  }
}
