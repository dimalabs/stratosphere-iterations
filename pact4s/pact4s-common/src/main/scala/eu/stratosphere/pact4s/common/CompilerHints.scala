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

package eu.stratosphere.pact4s.common

import scala.util.DynamicVariable

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.common.contract.DataDistribution
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.util.{ FieldSet => PactFieldSet }
import eu.stratosphere.pact.generic.contract.Contract

trait Hintable extends Serializable {

  private var _name: Option[String] = None

  def name = _name getOrElse null
  def name_=(value: String) = _name = Option(value)
  def name(value: String): this.type = { _name = Option(value); this }

  def applyHints(contract: Pact4sContract[_]): Unit = {
    _name.foreach(contract.setName)
  }
}

object Hintable {
  // this method is called by the compiler plugin's autonamer
  def withNameIfNotSet[T <: Hintable](that: T, name: String): T = {
    if (that != null && that.name == null)
      that.name = name
    that
  }
}

trait OutputHintable[Out] extends Hintable {

  private case class KeyCardinality(key: KeySelector[_ <: Function1[Out, _]], isUnique: Boolean, distinctCount: Option[Long], avgNumRecords: Option[Float]) {

    private class RefreshableFieldSet extends PactFieldSet {
      def refresh(indexes: Set[Int]) = {
        this.collection.clear()
        for (index <- indexes)
          this.add(index)
      }
    }

    @transient private var pactFieldSets = collection.mutable.Map[Pact4sContract[_], RefreshableFieldSet]()

    def getPactFieldSet(contract: Pact4sContract[_]): PactFieldSet = {

      if (pactFieldSets == null)
        pactFieldSets = collection.mutable.Map[Pact4sContract[_], RefreshableFieldSet]()
      
      val keyCopy = key.copy
      contract.getUDF.attachOutputsToInputs(keyCopy.inputFields)
      val keySet = keyCopy.selectedFields.toIndexSet

      val fieldSet = pactFieldSets.getOrElseUpdate(contract, new RefreshableFieldSet())
      fieldSet.refresh(keySet)
      fieldSet
    }
  }

  private var _degreeOfParallelism: Option[Int] = None
  private var _avgBytesPerRecord: Option[Float] = None
  private var _avgRecordsEmittedPerCall: Option[Float] = None
  private var _cardinalities: List[KeyCardinality] = List[KeyCardinality]()

  def degreeOfParallelism = _degreeOfParallelism getOrElse -1
  def degreeOfParallelism_=(value: Int) = _degreeOfParallelism = Some(value)
  def degreeOfParallelism(value: Int): this.type = { _degreeOfParallelism = Some(value); this }

  def avgBytesPerRecord = _avgBytesPerRecord getOrElse -1f
  def avgBytesPerRecord_=(value: Float) = _avgBytesPerRecord = Some(value)
  def avgBytesPerRecord(value: Float): this.type = { _avgBytesPerRecord = Some(value); this }

  def avgRecordsEmittedPerCall = _avgRecordsEmittedPerCall getOrElse -1f
  def avgRecordsEmittedPerCall_=(value: Float) = _avgRecordsEmittedPerCall = Some(value)
  def avgRecordsEmittedPerCall(value: Float): this.type = { _avgRecordsEmittedPerCall = Some(value); this }

  def uniqueKey[Key](key: KeySelector[Out => Key], distinctCount: Long = -1): this.type = {
    val optDistinctCount = if (distinctCount >= 0) Some(distinctCount) else None
    _cardinalities = KeyCardinality(key, true, optDistinctCount, None) :: _cardinalities
    this
  }

  def cardinality[Key](key: KeySelector[Out => Key], distinctCount: Long = -1, avgNumRecords: Float = -1): this.type = {
    val optDistinctCount = if (distinctCount >= 0) Some(distinctCount) else None
    val optAvgNumRecords = if (avgNumRecords >= 0) Some(avgNumRecords) else None
    _cardinalities = KeyCardinality(key, false, optDistinctCount, optAvgNumRecords) :: _cardinalities
    this
  }

  override def applyHints(contract: Pact4sContract[_]): Unit = {

    super.applyHints(contract)
    val hints = contract.getCompilerHints

    _degreeOfParallelism.foreach(contract.setDegreeOfParallelism)
    _avgBytesPerRecord.foreach(hints.setAvgBytesPerRecord)
    _avgRecordsEmittedPerCall.foreach(hints.setAvgRecordsEmittedPerStubCall)

    if (hints.getUniqueFields != null)
      hints.getUniqueFields.clear()
    hints.getDistinctCounts.clear()
    hints.getAvgNumRecordsPerDistinctFields.clear()

    _cardinalities.foreach { card =>

      val fieldSet = card.getPactFieldSet(contract)

      if (card.isUnique) {
        val fieldSets = hints.getUniqueFields
        if (fieldSets == null)
          hints.setUniqueField(fieldSet)
        else
          fieldSets.add(fieldSet)
      }

      card.distinctCount.foreach(hints.setDistinctCount(fieldSet, _))
      card.avgNumRecords.foreach(hints.setAvgNumRecordsPerDistinctFields(fieldSet, _))
    }
  }
}

trait InputHintable[In, Out] extends Serializable {

  private case class UnreadFields(fields: FieldSelector[_ <: Function1[In, _]], negate: Boolean)
  private case class CopiedFields(from: FieldSelector[_ <: Function1[In, _]], to: FieldSelector[_ <: Function1[Out, _]])

  private var _unread: Option[UnreadFields] = None
  private var _copied: Option[CopiedFields] = None

  def neglects[Fields](fields: FieldSelector[In => Fields]): Unit = {
    _unread = Some(UnreadFields(fields, false))
  }

  def observes[Fields](fields: FieldSelector[In => Fields]): Unit = {
    _unread = Some(UnreadFields(fields, true))
  }

  def preserves[Fields](from: FieldSelector[In => Fields]) = new {
    def as(to: FieldSelector[Out => Fields]): Unit = {
      _copied = Some(CopiedFields(from, to))
    }
  }

  protected def applyInputHints(markUnread: Int => Unit, markCopied: (Int, Int) => Unit): Unit = {

    if (InputHintable.enabled.value) {

      _unread map { unread =>

        val fieldSet = unread.fields.selectedFields.map(_.localPos).toSet
        val unreadFields = unread.negate match {
          // selected fields are unread
          case false => fieldSet
          // selected fields are read => all other fields are unread
          case true => unread.fields.inputFields.map(_.localPos).toSet.diff(fieldSet)
        }

        unreadFields.foreach(markUnread(_))
      }

      _copied map {
        case CopiedFields(from, to) => {
          val pairs = from.selectedFields.map(_.localPos).zip(to.selectedFields.map(_.localPos))
          pairs.foreach(markCopied.tupled)
        }
      }
    }
  }
}

object InputHintable {

  private val enabled = new DynamicVariable[Boolean](true)

  def withEnabled[T](isEnabled: Boolean)(thunk: => T): T = enabled.withValue(isEnabled) { thunk }
}

trait OneInputHintable[In, Out] extends InputHintable[In, Out] with OutputHintable[Out] {

  override def applyHints(contract: Pact4sContract[_]): Unit = {

    super.applyHints(contract)

    val udf = contract.getUDF.asInstanceOf[UDF1[In, Out]]
    applyInputHints(udf.markInputFieldUnread _, udf.markFieldCopied _)
  }
}

trait TwoInputHintable[LeftIn, RightIn, Out] extends OutputHintable[Out] {

  object left extends InputHintable[LeftIn, Out] {
    protected[TwoInputHintable] def applyInputHints(udf: UDF2[LeftIn, RightIn, Out]): Unit = {
      applyInputHints({ pos => udf.markInputFieldUnread(Left(pos)) }, { (from, to) => udf.markFieldCopied(Left(from), to) })
    }
  }

  object right extends InputHintable[RightIn, Out] {
    protected[TwoInputHintable] def applyInputHints(udf: UDF2[LeftIn, RightIn, Out]): Unit = {
      applyInputHints({ pos => udf.markInputFieldUnread(Right(pos)) }, { (from, to) => udf.markFieldCopied(Right(from), to) })
    }
  }

  override def applyHints(contract: Pact4sContract[_]): Unit = {

    super.applyHints(contract)

    val udf = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]]
    left.applyInputHints(udf)
    right.applyInputHints(udf)
  }
}

