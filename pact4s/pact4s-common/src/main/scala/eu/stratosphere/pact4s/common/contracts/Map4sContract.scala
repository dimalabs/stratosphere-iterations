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

  val userCode: Either[In => Out, In => Iterator[Out]]

  private def outCardBound = userCode.fold({ _ => Annotations.CARD_INPUTCARD }, { _ => Annotations.CARD_UNKNOWN })

  override def annotations = Seq(
    Annotations.getConstantFields(udf.getForwardIndexArray),
    Annotations.getOutCardBounds(outCardBound, outCardBound)
  /*
    Annotations.getReads(mapUDF.getReadFields),
    Annotations.getExplicitModifications(mapUDF.getWriteFields),
    Annotations.getImplicitOperation(ImplicitOperationMode.Copy),
    Annotations.getExplicitProjections(mapUDF.getDiscardedFields),
    */
  )

  override def persistConfiguration() = {

    val stubParameters = new MapParameters(
      udf.getInputDeserializer, 
      udf.getOutputSerializer, 
      udf.getDiscardIndexArray, 
      udf.getOutputLength, 
      userCode
    )
    stubParameters.persist(this)
  }
}

object Map4sContract {

  def newBuilder[In, Out] = MapContract.builder(classOf[Map4sStub[In, Out]])

  def unapply(c: Map4sContract[_, _]) = Some(c.singleInput)
}
