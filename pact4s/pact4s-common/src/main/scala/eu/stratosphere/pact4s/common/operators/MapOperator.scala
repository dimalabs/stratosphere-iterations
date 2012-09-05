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

  def map[Out: UDT](mapFunction: UDF1Code[In => Out]) = createStream(Left(mapFunction))

  def flatMap[Out: UDT](mapFunction: UDF1Code[In => Iterator[Out]]) = createStream(Right(mapFunction))

  def filter(predicate: UDF1Code[In => Boolean]) = {
    val pred = predicate.userFunction
    val fun = predUdfToMapUdf(predicate) { x => if (pred(x)) Iterator.single(x) else Iterator.empty }
    input flatMap fun
  }

  private def predUdfToMapUdf(predicateUDF: UDF1)(mapFunction: In => Iterator[In]): UDF1Code[In => Iterator[In]] = {

    val numFields = implicitly[UDT[In]].numFields

    val udf = new UDF1Code[In => Iterator[In]] with AnalyzedUDF1 {
      override val userFunction = mapFunction
      override val readFields = getInitialReadFields(numFields)
      override val writeFields = getInitialWriteFields(numFields)
    }

    for (i <- 0 until numFields) {
      if (predicateUDF.getReadFields(i) < 0) udf.markInputFieldUnread(i)
      udf.markInputFieldCopied(i, i)
    }

    udf
  }

  private def createStream[Out: UDT](mapFunction: Either[UDF1Code[In => Out], UDF1Code[In => Iterator[Out]]]): DataStream[Out] = new DataStream[Out] {

    override def createContract = {

      val builder = Map4sContract.newBuilder.input(input.getContract)
      
      new MapContract(builder) with Map4sContract[In, Out] {

        override val inputUDT = implicitly[UDT[In]]
        override val outputUDT = implicitly[UDT[Out]]
        override val mapUDF = mapFunction.fold(fun => fun: UDF1, fun => fun: UDF1)
        override val userFunction = mapFunction.fold(fun => Left(fun.userFunction), fun => Right(fun.userFunction))
      }
    }
  }
}

