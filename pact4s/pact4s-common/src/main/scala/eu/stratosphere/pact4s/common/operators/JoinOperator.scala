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

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class JoinOperator[LeftIn: UDT](leftInput: DataStream[LeftIn]) extends Serializable {

  def join[RightIn: UDT](rightInput: DataStream[RightIn]) = new Serializable {

    def on[Key](leftKeySelector: KeySelector[LeftIn => Key]) = new Serializable {

      def isEqualTo(rightKeySelector: KeySelector[RightIn => Key]) = new Serializable {

        def map[Out: UDT](mapFunction: (LeftIn, RightIn) => Out) = createStream(Left(mapFunction))

        def flatMap[Out: UDT](mapFunction: (LeftIn, RightIn) => Iterator[Out]) = createStream(Right(mapFunction))

        private def createStream[Out: UDT](mapFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]) = new DataStream[Out] with TwoInputHintable[LeftIn, RightIn, Out] {

          override def createContract = {

            val builder = Join4sContract.newBuilderFor(mapFunction).input1(leftInput.getContract).input2(rightInput.getContract)

            val keyTypes = implicitly[UDT[LeftIn]].getKeySet(leftKeySelector.selectedFields map { _.localPos })
            keyTypes.foreach { builder.keyField(_, -1, -1) } // global indexes haven't been computed yet...

            val contract = new MatchContract(builder) with Join4sContract[Key, LeftIn, RightIn, Out] {

              override val leftKey = leftKeySelector.copy()
              override val rightKey = rightKeySelector.copy()
              override val udf = new UDF2[LeftIn, RightIn, Out]
              override val userCode = mapFunction
            }

            applyHints(contract)
            contract
          }
        }
      }
    }
  }
}
