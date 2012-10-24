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

  protected case class Field(val pos: Int, val discard: Option[Boolean])

  private var globalized = false
  private var fields: Seq[(Int, Field)] = _

  protected def setInitialFields(selFields: Seq[(Int, Int)]) = fields = (selFields map { case (fieldNum, pos) => (fieldNum, Field(pos, None)) })

  override def isGlobalized = globalized

  override def getFields = fields map {
    case (fieldNum, Field(pos, None | Some(false))) => (fieldNum, pos)
    case (fieldNum, Field(_, Some(true)))           => (fieldNum, -1)
  }

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != globalized)
      throw new IllegalStateException()
  }

  override def globalize(locations: Map[Int, Int]) = {

    if (!globalized) {
      fields = fields map { case (local, _) => (local, Field(locations(local), None)) }
      globalized = true
    }
  }

  override def relocateField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    fields = fields map {
      case (fieldNum, Field(pos, dis)) if pos == oldPosition => (fieldNum, Field(newPosition, dis))
      case field                                             => field
    }
  }

  override def discardUnusedFields(used: Set[Int]) = {

    assertGlobalized(true)

    fields = fields map {

      case (fieldNum, Field(pos, discard)) => {
        val newDiscard = discard match {
          case None | Some(true) => Some(!used.contains(pos))
          case Some(false)       => Some(false)
        }
        (fieldNum, Field(pos, newDiscard))
      }
    }
  }
}

object AnalyzedFieldSelector {

  def apply[T1: UDT, R](fun: T1 => R): FieldSelectorCode[T1 => R] = apply[T1, R](fun, (0 until implicitly[UDT[T1]].numFields).toList)
  def apply[T1: UDT, R](fun: T1 => R, selFields: List[Int]): FieldSelectorCode[T1 => R] = apply[T1, R](fun, selFields map { x => (x, x) })
  def apply[T1: UDT, R](fun: T1 => R, selFields: Seq[(Int, Int)]): FieldSelectorCode[T1 => R] = new FieldSelectorImpl[T1, R](selFields)

  def apply[T1, R](udt: UDT[T1]): FieldSelectorCode[T1 => R] = apply[T1, R](null: T1 => R)(udt)
  def apply[T1, R](udt: UDT[T1], selFields: List[Int]): FieldSelectorCode[T1 => R] = apply[T1, R](null: T1 => R, selFields)(udt)
  def apply[T1, R](udt: UDT[T1], selections: Seq[Seq[String]]): FieldSelectorCode[T1 => R] = apply[T1, R](null: T1 => R, udt.getFieldIndexes(selections))(udt)

  def fromIndexMap[T1, R](udt: UDT[T1], indexMap: Array[Int]): FieldSelectorCode[T1 => R] = {
    val selFields = indexMap.zipWithIndex.toSeq.map(_.swap)
    new FieldSelectorImpl[T1, R](selFields)
  }

  def toIndexMap[T1: UDT, R](fieldSelector: FieldSelectorCode[T1 => R]): Array[Int] = {
    val numFields = implicitly[UDT[T1]].numFields
    val indexMap = (0 until numFields).toArray
    val fields = fieldSelector.getFields.toMap

    for (fieldNum <- 0 until numFields)
      indexMap(fieldNum) = fields.getOrElse(fieldNum, -1)

    indexMap
  }

  private class FieldSelectorImpl[T1, R](selFields: Seq[(Int, Int)]) extends FieldSelectorCode[T1 => R] with AnalyzedFieldSelector {
    setInitialFields(selFields)
  }
}

