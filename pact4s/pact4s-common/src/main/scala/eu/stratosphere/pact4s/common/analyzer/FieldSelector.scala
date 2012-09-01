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

package eu.stratosphere.pact4s.common.analyzer

import scala.reflect.Code

trait FieldSelector extends Serializable {

  def isGlobalized: Boolean
  def getFields: Array[Int]
  def getGlobalFields: Map[Int, Int] = getFields.zipWithIndex.map(_.swap).filter(_._2 >= 0).toMap

  def markFieldUnused(inputFieldNum: Int)

  def globalize(locations: Map[Int, Int])
  def relocateField(oldPosition: Int, newPosition: Int)
}

trait FieldSelectorCode[T] extends FieldSelector.EmptyCode[T] with FieldSelector

trait FieldSelectorLowPriorityImplicits {

  class FieldSelectorAnalysisFailedException extends RuntimeException("Field selector analysis failed. This should have been caught at compile time.")

  implicit def unanalyzedFieldSelector[T1, R](fun: T1 => R): FieldSelectorCode[T1 => R] = throw new FieldSelectorAnalysisFailedException
  implicit def unanalyzedFieldSelectorCode[T1, R](fun: Code[T1 => R]): FieldSelectorCode[T1 => R] = throw new FieldSelectorAnalysisFailedException
}

object FieldSelector extends FieldSelectorLowPriorityImplicits {

  abstract class EmptyCode[T] extends Code[T](null) {
    @transient override val tree = null
  }
}