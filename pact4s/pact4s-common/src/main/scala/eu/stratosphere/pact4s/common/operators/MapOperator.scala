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

class MapOperator[In: UDT](input: DataStream[In]) extends Serializable {

  def map[Out: UDT](mapFunction: In => Out) = createStream(Left(mapFunction))

  def flatMap[Out: UDT](mapFunction: In => Iterator[Out]) = createStream(Right(mapFunction))

  def filter(predicate: In => Boolean) = {

    val stream = input flatMap { x => if (predicate(x)) Iterator.single(x) else Iterator.empty }

    def allFields = new FieldSelector[In => In](implicitly[UDT[In]])

    stream preserves allFields as allFields
    stream
  }

  private def createStream[Out: UDT](mapFunction: Either[In => Out, In => Iterator[Out]]) = new DataStream[Out] with OneInputHintable[In, Out] {

    override def createContract = {

      val builder = Map4sContract.newBuilderFor(mapFunction).input(input.getContract)

      val contract = new MapContract(builder) with Map4sContract[In, Out] {

        override val udf = new UDF1[In, Out]
        override val userCode = mapFunction
      }

      applyHints(contract)
      contract
    }
  }
}

