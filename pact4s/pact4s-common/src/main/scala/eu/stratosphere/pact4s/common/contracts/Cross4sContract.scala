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

trait Cross4sContract[LeftIn, RightIn, Out] extends Pact4sTwoInputContract { this: CrossContract =>

  val leftUDT: UDT[LeftIn]
  val rightUDT: UDT[RightIn]
  val outputUDT: UDT[Out]
  val crossUDF: UDF2[(LeftIn, RightIn) => _]
  val userFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]

  override def annotations = Seq(
    Annotations.getConstantFieldsFirstExcept(crossUDF.getWriteFields ++ crossUDF.getDiscardedFields._1),
    Annotations.getConstantFieldsSecondExcept(crossUDF.getWriteFields ++ crossUDF.getDiscardedFields._2)
  /*
    Annotations.getReadsFirst(crossUDF.getReadFields._1),
    Annotations.getReadsSecond(crossUDF.getReadFields._2),
    Annotations.getExplicitModifications(crossUDF.getWriteFields),
    Annotations.getImplicitOperationFirst(ImplicitOperationMode.Copy),
    Annotations.getImplicitOperationSecond(ImplicitOperationMode.Copy),
    Annotations.getExplicitProjectionsFirst(crossUDF.getDiscardedFields._1),
    Annotations.getExplicitProjectionsSecond(crossUDF.getDiscardedFields._2)
    */
  )

  override def persistConfiguration() = {

    val leftDeserializer = leftUDT.getSerializer(crossUDF.getReadFields._1)
    val leftDiscard = crossUDF.getDiscardedFields._1
    val rightDeserializer = rightUDT.getSerializer(crossUDF.getReadFields._2)
    val rightDiscard = crossUDF.getDiscardedFields._2
    val serializer = outputUDT.getSerializer(crossUDF.getWriteFields)

    val stubParameters = new CrossParameters(leftDeserializer, leftDiscard, rightDeserializer, rightDiscard, serializer, userFunction)
    stubParameters.persist(this)
  }
}

object Cross4sContract {

  def getStub[LeftIn, RightIn, Out] = classOf[Cross4sStub[LeftIn, RightIn, Out]]

  def unapply(c: Cross4sContract[_, _, _]) = Some((c.leftInput, c.rightInput, c.leftUDT, c.rightUDT, c.outputUDT, c.crossUDF))
}