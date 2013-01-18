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

package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Cross4sContract[LeftIn, RightIn, Out] extends Pact4sTwoInputContract[LeftIn, RightIn, Out] { this: CrossContract =>

  val userCode: CrossParameters.FunType[LeftIn, RightIn, Out]

  override def annotations = Seq(
    Annotations.getConstantFieldsFirst(udf.getLeftForwardIndexArray),
    Annotations.getConstantFieldsSecond(udf.getRightForwardIndexArray)
  )

  override def persistConfiguration() = {

    val stubParameters = CrossParameters(
      udf.getLeftInputDeserializer, udf.getLeftDiscardIndexArray.filter(_ < udf.getOutputLength),
      udf.getRightInputDeserializer, udf.getRightForwardIndexArray,
      udf.getOutputSerializer, udf.getOutputLength, userCode
    )
    stubParameters.persist(this)
  }
}

object Cross4sContract {

  def newBuilderFor[LeftIn, RightIn, Out](mapFunction: CrossParameters.FunType[LeftIn, RightIn, Out]) = CrossContract.builder(CrossParameters.getStubFor(mapFunction))

  def unapply(c: Cross4sContract[_, _, _]) = Some((c.leftInput, c.rightInput))
}