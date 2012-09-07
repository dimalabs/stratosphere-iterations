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

class ReduceOperator[In: UDT](input: DataStream[In]) extends Serializable {

  def groupBy[Key](keySelector: FieldSelectorCode[In => Key]) = new Serializable {

    def reduce[Out: UDT](reduceFunction: UDF1Code[Iterator[In] => Out]): DataStream[Out] = new ReduceStream(input, keySelector, None, reduceFunction.userFunction, null, reduceFunction)

    def combine(combineFunction: UDF1Code[Iterator[In] => In]): DataStream[In] = new ReduceStream(input, keySelector, Some(combineFunction.userFunction), combineFunction.userFunction, combineFunction, combineFunction) {

      private val outer = this

      def reduce[Out: UDT](reduceFunction: UDF1Code[Iterator[In] => Out]): DataStream[Out] = new ReduceStream(input, keySelector, Some(combineFunction.userFunction), reduceFunction.userFunction, combineFunction, reduceFunction) {

        override def getHints = outer.getGenericHints ++ this.hints
      }
    }
  }

  private class ReduceStream[Out: UDT](
    input: DataStream[In],
    keySelector: FieldSelector,
    combineFunction: Option[Iterator[In] => In],
    reduceFunction: Iterator[In] => Out,
    combineUDF: UDF1,
    reduceUDF: UDF1)
    extends DataStream[Out] {

    override def createContract = {

      val keyFields = keySelector.getFields
      val keyFieldTypes = implicitly[UDT[In]].getKeySet(keyFields map { _._1 })

      val udfs = {
        if (combineUDF eq reduceUDF)
          (combineUDF, combineUDF.copy())
        else
          (combineUDF, reduceUDF)
      }

      val builder = Reduce4sContract.newBuilder.input(input.getContract)

      for ((keyType, (_, keyField)) <- (keyFieldTypes, keyFields).zipped.toList) {
        builder.keyField(keyType, keyField)
      }

      new ReduceContract(builder) with Reduce4sContract[In, Out] {

        override val keySelector = ReduceStream.this.keySelector
        override val inputUDT = implicitly[UDT[In]]
        override val outputUDT = implicitly[UDT[Out]]
        override val (combineUDF, reduceUDF) = udfs
        override val userCombineFunction = combineFunction
        override val userReduceFunction = reduceFunction
      }
    }
  }
}
