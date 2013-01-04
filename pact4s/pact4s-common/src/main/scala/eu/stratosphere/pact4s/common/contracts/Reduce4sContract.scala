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

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Reduce4sContract[Key, In, Out] extends Pact4sOneInputKeyedContract[Key, In, Out] { this: ReduceContract =>

  val combineUDF: UDF1[In, In]
  val userCombineCode: Option[Iterator[In] => In]
  val userReduceCode: Iterator[In] => Out

  private def combinableAnnotation = userCombineCode map { _ => Annotations.getCombinable() } toSeq
  //private def getAllReadFields = (combineUDF.getReadFields ++ reduceUDF.getReadFields).distinct.toArray

  override def annotations = combinableAnnotation ++ Seq(
    Annotations.getConstantFields(udf.getForwardIndexArray),
    Annotations.getOutCardBounds(Annotations.CARD_UNBOUNDED, Annotations.CARD_INPUTCARD)
  /*
    Annotations.getReads(getAllReadFields),
    Annotations.getExplicitModifications(reduceUDF.getWriteFields),
    Annotations.getImplicitOperation(ImplicitOperationMode.Projection),
    Annotations.getExplicitCopies(reduceUDF.getForwardedFields),
    */
  )

  override def persistConfiguration() = {

    val combineDeserializer = userCombineCode map { _ => combineUDF.getInputDeserializer }
    val combineSerializer = userCombineCode map { _ => combineUDF.getOutputSerializer }

    val reduceDeserializer = udf.getInputDeserializer
    val reduceSerializer = udf.getOutputSerializer
    
    val forward = udf.getForwardIndexArray

    val stubParameters = new ReduceParameters(
      combineDeserializer, combineSerializer, userCombineCode, 
      reduceDeserializer, reduceSerializer, userReduceCode,
      forward
    )
    stubParameters.persist(this)
  }
}

object Reduce4sContract {

  def newBuilder[In, Out] = ReduceContract.builder(classOf[Reduce4sStub[In, Out]])

  def unapply(c: Reduce4sContract[_, _, _]) = Some(c.singleInput)
}