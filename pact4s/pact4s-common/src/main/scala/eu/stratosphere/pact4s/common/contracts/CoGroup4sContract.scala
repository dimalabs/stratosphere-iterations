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

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait CoGroup4sContract[LeftIn, RightIn, Out] extends Pact4sTwoInputContract { this: CoGroupContract =>

  val leftKeySelector: FieldSelector
  val rightKeySelector: FieldSelector
  val leftUDT: UDT[LeftIn]
  val rightUDT: UDT[RightIn]
  val outputUDT: UDT[Out]
  val coGroupUDF: UDF2
  val userFunction: Either[(Iterator[LeftIn], Iterator[RightIn]) => Out, (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]

  override def annotations = Seq(
    Annotations.getConstantFieldsFirst(coGroupUDF.getForwardedFields._1),
    Annotations.getConstantFieldsSecond(coGroupUDF.getForwardedFields._2)
  /*
    Annotations.getReadsFirst(coGroupUDF.getReadFields._1),
    Annotations.getReadsSecond(coGroupUDF.getReadFields._2),
    Annotations.getExplicitModifications(coGroupUDF.getWriteFields),
    Annotations.getImplicitOperationFirst(ImplicitOperationMode.Projection),
    Annotations.getImplicitOperationSecond(ImplicitOperationMode.Projection),
    Annotations.getExplicitCopiesFirst(coGroupUDF.getForwardedFields._1),
    Annotations.getExplicitCopiesSecond(coGroupUDF.getForwardedFields._2)
    */
  )

  override def persistConfiguration() = {

    val leftDeserializer = leftUDT.getSerializer(coGroupUDF.getReadFields._1)
    val leftForward = coGroupUDF.getForwardedFields._1
    val rightDeserializer = rightUDT.getSerializer(coGroupUDF.getReadFields._2)
    val rightForward = coGroupUDF.getForwardedFields._2
    val serializer = outputUDT.getSerializer(coGroupUDF.getWriteFields)

    val stubParameters = CoGroupParameters(leftDeserializer, leftForward, rightDeserializer, leftForward, serializer, userFunction)
    stubParameters.persist(this)
  }
}

object CoGroup4sContract {

  def newBuilder[LeftIn, RightIn, Out] = CoGroupContract.builder(classOf[CoGroup4sStub[LeftIn, RightIn, Out]])

  def unapply(c: CoGroup4sContract[_, _, _]) = Some((c.leftInput, c.rightInput, c.leftKeySelector, c.rightKeySelector, c.leftUDT, c.rightUDT, c.outputUDT, c.coGroupUDF))
}