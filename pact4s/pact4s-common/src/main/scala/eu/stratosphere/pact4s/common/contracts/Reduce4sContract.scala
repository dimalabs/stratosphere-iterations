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

  val userCombineCode: Option[Iterator[In] => In]
  val userReduceCode: Iterator[In] => Out
  
  private def ignoredKeys: Set[Int] = {
    val ignoredInputs = udf.inputFields.filterNot(_.isUsed).map(_.globalPos.getValue).toSet
    key.selectedFields.toIndexSet.intersect(ignoredInputs)
  }
  
  def combineForwardSet: Set[Int] = udf.forwardSet.map(_.getValue).union(ignoredKeys).toSet
  def combineDiscardSet: Set[Int] = udf.discardSet.map(_.getValue).diff(ignoredKeys).diff(udf.inputFields.toIndexSet).toSet

  private def combinableAnnotation = userCombineCode map { _ => Annotations.getCombinable() } toSeq
  //private def getAllReadFields = (combineUDF.getReadFields ++ reduceUDF.getReadFields).distinct.toArray

  override def annotations = combinableAnnotation ++ Seq(
    Annotations.getConstantFields(udf.getForwardIndexArray._1),
    Annotations.getOutCardBounds(Annotations.CARD_UNBOUNDED, Annotations.CARD_INPUTCARD)
  /*
    Annotations.getReads(getAllReadFields),
    Annotations.getExplicitModifications(reduceUDF.getWriteFields),
    Annotations.getImplicitOperation(ImplicitOperationMode.Projection),
    Annotations.getExplicitCopies(reduceUDF.getForwardedFields),
    */
  )

  override def persistConfiguration() = {
    
    val combineForwardArray = combineForwardSet.toArray
    val combineForwardTypes = combineForwardArray map { gPos =>
      val lPos = udf.inputFields.find(_.globalPos.getValue == gPos).get.localPos
      udf.inputUDT.fieldTypes(lPos)
    }

    val stubParameters = new ReduceParameters(
      udf.getInputDeserializer, udf.getOutputSerializer, 
      userCombineCode, (combineForwardArray, combineForwardTypes), 
      userReduceCode,  udf.getForwardIndexArray
    )
    stubParameters.persist(this)
  }
}

object Reduce4sContract {

  def newBuilder[In, Out] = ReduceContract.builder(classOf[Reduce4sStub[In, Out]])

  def unapply(c: Reduce4sContract[_, _, _]) = Some(c.singleInput)
}