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

class MapOperator[In: UDT](input: DataStream[In]) extends Serializable {

  def map[Out: UDT, F: UDF1Builder[In, Out]#UDF](mapFunction: In => Out) = createStream(Left(mapFunction))

  def flatMap[Out: UDT, F: UDF1Builder[In, Iterator[Out]]#UDF](mapFunction: In => Iterator[Out]) = createStream(Right(mapFunction))

  def filter[F: SelectorBuilder[In, Boolean]#Selector](predicate: In => Boolean) = {

    val reads = implicitly[FieldSelector[In => Boolean]].getFields
    val udt = implicitly[UDT[In]]

    implicit val udf = new AnalyzedUDF1[In, Iterator[In]](udt.numFields, udt.numFields) {
      for (i <- 0 until udt.numFields) {
        if (reads(i) < 0) markInputFieldUnread(i)
        markInputFieldCopied(i, i)
      }
    }

    input flatMap { x => if (predicate(x)) Iterator.single(x) else Iterator.empty }
  }

  private def createStream[Out: UDT, R, F: UDF1Builder[In, R]#UDF](
    mapFunction: Either[In => Out, In => Iterator[Out]]): DataStream[Out] = new DataStream[Out] {

    override def createContract = {

      new MapContract(Map4sContract.getStub, input.getContract) with Map4sContract[In, Out] {

        override val inputUDT = implicitly[UDT[In]]
        override val outputUDT = implicitly[UDT[Out]]
        override val mapUDF = implicitly[UDF1[In => R]]
        override val userFunction = mapFunction
      }
    }
  }
}

