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
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class ReduceOperator[In: UDT](input: DataStream[In]) extends Serializable {

  def groupBy[Key: UDT](keySelector: KeySelector[In => Key]) = new Serializable {

    def reduce[Out: UDT](reduceFunction: Iterator[In] => Out): DataStream[Out] = new ReduceStream(input, keySelector, None, reduceFunction)

    def combine(combineFunction: Iterator[In] => In): DataStream[In] = new ReduceStream(input, keySelector, Some(combineFunction), combineFunction) {

      private val outer = this

      def reduce[Out: UDT](reduceFunction: Iterator[In] => Out): DataStream[Out] = new ReduceStream(input, keySelector, Some(combineFunction), reduceFunction) {

        override def getHints = outer.getGenericHints ++ this.hints
      }
    }
  }

  private class ReduceStream[Key: UDT, Out: UDT](
    input: DataStream[In],
    keySelector: KeySelector[In => Key],
    combineFunction: Option[Iterator[In] => In],
    reduceFunction: Iterator[In] => Out)
    extends DataStream[Out] {

    override def createContract = {

      val builder = Reduce4sContract.newBuilder.input(input.getContract)

      val keyTypes = implicitly[UDT[In]].getKeySet(keySelector.selectedFields map { _.localPos })
      keyTypes.foreach { builder.keyField(_, -1) } // global indexes haven't been computed yet...

      new ReduceContract(builder) with Reduce4sContract[Key, In, Out] {

        override val key = keySelector
        val combineUDF = new UDF1[In, In]
        override val udf = new UDF1[In, Out]
        override val userCombineCode = combineFunction
        override val userReduceCode = reduceFunction
      }
    }
  }
}
