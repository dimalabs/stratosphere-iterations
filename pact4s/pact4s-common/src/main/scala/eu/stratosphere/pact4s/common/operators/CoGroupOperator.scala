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

    def on[Key](leftKeySelector: FieldSelectorCode[LeftIn => Key]) = new Serializable {

      def isEqualTo(rightKeySelector: FieldSelectorCode[RightIn => Key]) = new Serializable {

        def map[Out: UDT](mapFunction: UDF2Code[(Iterator[LeftIn], Iterator[RightIn]) => Out]) = createStream(Left(mapFunction))

        def flatMap[Out: UDT](mapFunction: UDF2Code[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]) = createStream(Right(mapFunction))

        private def createStream[Out: UDT](mapFunction: Either[UDF2Code[(Iterator[LeftIn], Iterator[RightIn]) => Out], UDF2Code[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]]): DataStream[Out] = new DataStream[Out] {

          override def createContract = {

            val leftKeyFieldSelector: FieldSelector = leftKeySelector
            val rightKeyFieldSelector: FieldSelector = rightKeySelector
            val leftKeyFields = leftKeyFieldSelector.getFields
            val rightKeyFields = rightKeyFieldSelector.getFields
            val keyFieldTypes = implicitly[UDT[LeftIn]].getKeySet(leftKeyFields map { _._1 })

            val builder = CoGroup4sContract.newBuilder.input1(leftInput.getContract).input2(rightInput.getContract)
            
            for ((keyType, (_, leftKey), (_, rightKey)) <- (keyFieldTypes, leftKeyFields, rightKeyFields).zipped.toList) {
              builder.keyField(keyType, leftKey, rightKey)
            }
            
            new CoGroupContract(builder) with CoGroup4sContract[LeftIn, RightIn, Out] {

              override val leftKeySelector = leftKeyFieldSelector
              override val rightKeySelector = rightKeyFieldSelector
              override val leftUDT = implicitly[UDT[LeftIn]]
              override val rightUDT = implicitly[UDT[RightIn]]
              override val outputUDT = implicitly[UDT[Out]]
              override val coGroupUDF = mapFunction.fold(fun => fun: UDF2, fun => fun: UDF2)
              override val userFunction = mapFunction.fold(fun => Left(fun.userFunction), fun => Right(fun.userFunction))
            }
          }
        }
      }
    }
  }
}
