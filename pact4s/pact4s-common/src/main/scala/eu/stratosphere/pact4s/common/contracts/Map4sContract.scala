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

package eu.stratosphere.pact4s.common.contracts

import java.lang.annotation.Annotation

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Map4sContract[In, Out] extends Pact4sOneInputContract { this: MapContract =>

  val inputUDT: UDT[In]
  val outputUDT: UDT[Out]
  val mapUDF: UDF1[In => _]
  val userFunction: Either[In => Out, In => Iterator[Out]]

  private def outCardBound = userFunction.fold({ _ => Annotations.CARD_INPUTCARD }, { _ => Annotations.CARD_UNKNOWN })

  override def annotations = Seq(
    Annotations.getConstantFieldsExcept(mapUDF.getWriteFields ++ mapUDF.getDiscardedFields),
    Annotations.getOutCardBounds(outCardBound, outCardBound)
  /*
    Annotations.getReads(mapUDF.getReadFields),
    Annotations.getExplicitModifications(mapUDF.getWriteFields),
    Annotations.getImplicitOperation(ImplicitOperationMode.Copy),
    Annotations.getExplicitProjections(mapUDF.getDiscardedFields),
    */
  )

  override def persistConfiguration() = {

    val deserializer = inputUDT.createSerializer(mapUDF.getReadFields)
    val serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
    val discard = mapUDF.getDiscardedFields

    val stubParameters = new MapParameters(deserializer, serializer, discard, userFunction)
    stubParameters.persist(this)
  }
}

object Map4sContract {

  def getStub[In, Out] = classOf[Map4sStub[In, Out]]

  def unapply(c: Map4sContract[_, _]) = Some((c.singleInput, c.inputUDT, c.outputUDT, c.mapUDF))
}
