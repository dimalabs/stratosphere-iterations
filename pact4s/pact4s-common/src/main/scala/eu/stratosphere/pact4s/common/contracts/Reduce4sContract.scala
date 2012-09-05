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

trait Reduce4sContract[In, Out] extends Pact4sOneInputContract { this: ReduceContract =>

  val keySelector: FieldSelector
  val inputUDT: UDT[In]
  val outputUDT: UDT[Out]
  val combineUDF: UDF1
  val reduceUDF: UDF1
  val userCombineFunction: Option[Iterator[In] => In]
  val userReduceFunction: Iterator[In] => Out

  private def combinableAnnotation = userCombineFunction map { _ => Annotations.getCombinable() } toSeq
  //private def getAllReadFields = (combineUDF.getReadFields ++ reduceUDF.getReadFields).distinct.toArray

  override def annotations = combinableAnnotation ++ Seq(
    Annotations.getConstantFields(reduceUDF.getForwardedFields),
    Annotations.getOutCardBounds(Annotations.CARD_UNBOUNDED, Annotations.CARD_INPUTCARD)
  /*
    Annotations.getReads(getAllReadFields),
    Annotations.getExplicitModifications(reduceUDF.getWriteFields),
    Annotations.getImplicitOperation(ImplicitOperationMode.Projection),
    Annotations.getExplicitCopies(reduceUDF.getForwardedFields),
    */
  )

  override def persistConfiguration() = {

    val combineDeserializer = userCombineFunction map { _ => inputUDT.getSerializer(combineUDF.getReadFields) }
    val combineSerializer = userCombineFunction map { _ => inputUDT.getSerializer(combineUDF.getWriteFields) }
    val combineForward = userCombineFunction map { _ => combineUDF.getForwardedFields }

    val reduceDeserializer = inputUDT.getSerializer(reduceUDF.getReadFields)
    val reduceSerializer = outputUDT.getSerializer(reduceUDF.getWriteFields)
    val reduceForward = reduceUDF.getForwardedFields

    val stubParameters = new ReduceParameters(combineDeserializer, combineSerializer, combineForward, userCombineFunction, reduceDeserializer, reduceSerializer, reduceForward, userReduceFunction)
    stubParameters.persist(this)
  }
}

object Reduce4sContract {

  def newBuilder[In, Out] = ReduceContract.builder(classOf[Reduce4sStub[In, Out]])

  def unapply(c: Reduce4sContract[_, _]) = Some((c.singleInput, c.keySelector, c.inputUDT, c.outputUDT, c.combineUDF, c.reduceUDF))
}