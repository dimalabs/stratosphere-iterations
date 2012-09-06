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

trait AnalyzedFieldSelector extends FieldSelector {

  private var globalized = false
  protected val fields: Array[Int]

  protected def getInitialFields(fieldCount: Int): Array[Int] = (0 until fieldCount).toArray

  override def isGlobalized = globalized
  override def getFields = fields

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != globalized)
      throw new IllegalStateException()
  }

  override def markFieldUnused(inputFieldNum: Int) = {
    assertGlobalized(false)
    fields(inputFieldNum) = -1
  }

  override def globalize(locations: Map[Int, Int]) = {

    if (!globalized) {

      for (fieldNum <- 0 until fields.length if fields(fieldNum) >= 0) {
        fields(fieldNum) = locations(fieldNum)
      }

      globalized = true
    }
  }

  override def relocateField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    for (fieldNum <- 0 until fields.length if fields(fieldNum) == oldPosition) {
      fields(fieldNum) = newPosition
    }
  }
}

object AnalyzedFieldSelector {

  def apply[T1: UDT, R](fun: T1 => R): FieldSelectorCode[T1 => R] = new FieldSelectorImpl[T1, R](implicitly[UDT[T1]].numFields)

  def apply[T1: UDT, R](fun: T1 => R, selFields: Set[Int]): FieldSelectorCode[T1 => R] = {
    val inputLength = implicitly[UDT[T1]].numFields
    val sel = new FieldSelectorImpl[T1, R](inputLength)
    ((0 until inputLength).toSet diff selFields) foreach { sel.markFieldUnused(_) }
    sel
  }

  private class FieldSelectorImpl[T1, R](fieldCount: Int) extends FieldSelectorCode[T1 => R] with AnalyzedFieldSelector {
    override val fields = getInitialFields(fieldCount)
  }

  def apply[T1, R](udt: UDT[T1]): FieldSelectorCode[T1 => R] = apply[T1, R](null: T1 => R)(udt)
  def apply[T1, R](udt: UDT[T1], selFields: Set[Int]): FieldSelectorCode[T1 => R] = apply[T1, R](null: T1 => R, selFields)(udt)
  def apply[T1, R](udt: UDT[T1], selections: Seq[Seq[String]]): FieldSelectorCode[T1 => R] = apply[T1, R](null: T1 => R, selections.toSet flatMap udt.getFieldIndex)(udt)
}

