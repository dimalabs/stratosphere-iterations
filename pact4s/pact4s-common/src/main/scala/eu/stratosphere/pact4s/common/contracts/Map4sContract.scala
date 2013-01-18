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

import java.lang.annotation.Annotation

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Map4sContract[In, Out] extends Pact4sOneInputContract[In, Out] { this: MapContract =>

  val userCode: MapParameters.FunType[In, Out]

  private def outCardBound = userCode.fold({ _ => Annotations.CARD_INPUTCARD }, { _ => Annotations.CARD_UNKNOWN })

  override def annotations = Seq(
    Annotations.getConstantFields(udf.getForwardIndexArray),
    Annotations.getOutCardBounds(outCardBound, outCardBound)
  )

  override def persistConfiguration() = {

    val stubParameters = MapParameters(
      udf.getInputDeserializer,
      udf.getOutputSerializer,
      udf.getDiscardIndexArray.filter(_ < udf.getOutputLength),
      udf.getOutputLength,
      userCode
    )
    stubParameters.persist(this)
  }
}

object Map4sContract {

  def newBuilderFor[In, Out](mapFunction: MapParameters.FunType[In, Out]) = MapContract.builder(MapParameters.getStubFor(mapFunction))

  def unapply(c: Map4sContract[_, _]) = Some(c.singleInput)
}
