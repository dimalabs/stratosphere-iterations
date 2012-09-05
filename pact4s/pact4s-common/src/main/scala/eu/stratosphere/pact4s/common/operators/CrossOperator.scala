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

class CrossOperator[LeftIn: UDT](leftInput: DataStream[LeftIn]) extends Serializable {

  def cross[RightIn: UDT](rightInput: DataStream[RightIn]) = new Serializable {

    def map[Out: UDT](mapFunction: UDF2Code[(LeftIn, RightIn) => Out]) = createStream(Left(mapFunction))

    def flatMap[Out: UDT](mapFunction: UDF2Code[(LeftIn, RightIn) => Iterator[Out]]) = createStream(Right(mapFunction))

    private def createStream[Out: UDT](
      mapFunction: Either[UDF2Code[(LeftIn, RightIn) => Out], UDF2Code[(LeftIn, RightIn) => Iterator[Out]]]): DataStream[Out] = new DataStream[Out] {

      override def createContract = {

        val builder = Cross4sContract.newBuilder.input1(leftInput.getContract).input2(rightInput.getContract)
        
        new CrossContract(builder) with Cross4sContract[LeftIn, RightIn, Out] {

          override val leftUDT = implicitly[UDT[LeftIn]]
          override val rightUDT = implicitly[UDT[RightIn]]
          override val outputUDT = implicitly[UDT[Out]]
          override val crossUDF = mapFunction.fold(fun => fun: UDF2, fun => fun: UDF2)
          override val userFunction = mapFunction.fold(fun => Left(fun.userFunction), fun => Right(fun.userFunction))
        }
      }
    }
  }
}

