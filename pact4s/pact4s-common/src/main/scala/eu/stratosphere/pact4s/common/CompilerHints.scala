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

package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analysis._

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.pact.common.contract.DataDistribution
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.util.FieldSet

trait HasHints[T] {
  
  def getHints: Seq[CompilerHint[T]]
  def getGenericHints = getHints filter { classOf[CompilerHint[Nothing]].isInstance(_) } map { _.asInstanceOf[CompilerHint[Nothing]] }
}

trait Hintable[T] extends HasHints[T] {

  var hints: Seq[CompilerHint[T]] = Seq()
  def getHints = hints
}

abstract class CompilerHint[+T] {
  def applyToContract(contract: Contract)
}

object CompilerHint {
  implicit def hint2SeqHint[T](h: CompilerHint[T]): Seq[CompilerHint[T]] = Seq(h)
}

case class PactName(val pactName: String) extends CompilerHint[Nothing] {
  override def applyToContract(contract: Contract) = contract.setName(pactName)
}

object PactName {
  // this method is called by the compiler plugin's autonamer
  def withNameIfNoHints[T <: Hintable[_]](that: T, name: String): T = {
    if (that != null && (that.hints == null || that.hints.isEmpty))
      that.hints = Seq(PactName(name))
    that
  }
}

case class Degree(val degreeOfParallelism: Int) extends CompilerHint[Nothing] {
  override def applyToContract(contract: Contract) = contract.setDegreeOfParallelism(degreeOfParallelism)
}

case class RecordSize(val avgSizeInBytes: Float) extends CompilerHint[Nothing] {
  override def applyToContract(contract: Contract) = contract.getCompilerHints().setAvgBytesPerRecord(avgSizeInBytes)
}

case class RecordsEmitted(val avgNumRecords: Float) extends CompilerHint[Nothing] {
  override def applyToContract(contract: Contract) = contract.getCompilerHints().setAvgRecordsEmittedPerStubCall(avgNumRecords)
}

case class UniqueKey[T: UDT, Key](val keySelector: KeySelector[T => Key]) extends CompilerHint[T] {

  override def applyToContract(contract: Contract) = {

    val fieldSet = new FieldSet(keySelector.selectedFields.map(_.globalPos.getValue).toArray)
    val hints = contract.getCompilerHints()

    val fieldSets = hints.getUniqueFields()
    if (fieldSets == null)
      hints.setUniqueField(fieldSet)
    else
      fieldSets.add(fieldSet)
  }
}

case class KeyCardinality[T: UDT, Key](val keySelector: KeySelector[T => Key], val numDistinctKeys: Long, val avgNumRecordsPerKey: Option[Long] = None) extends CompilerHint[T] {

  override def applyToContract(contract: Contract) = {

    val fieldSet = new FieldSet(keySelector.selectedFields.map(_.globalPos.getValue).toArray)
    val hints = contract.getCompilerHints()
    hints.setDistinctCount(fieldSet, numDistinctKeys)

    if (avgNumRecordsPerKey.isDefined)
      hints.setAvgNumRecordsPerDistinctFields(fieldSet, avgNumRecordsPerKey.get)
  }
}

// TODO: Ask Fabian why CompilerHints.setInputDistributionClass was deprecated (has it been replaced by something else?)
/*
case class InputDistribution(dataDistribution: Class[_ <: InputDataDistribution[_]]) extends CompilerHint[Nothing] {
  override def applyToContract(contract: Contract) = contract.getCompilerHints().setInputDistributionClass(dataDistribution)
}
*/

abstract class InputDataDistribution[Key: UDT] extends DataDistribution {

  def getBucketUpperBound(bucketNum: Int, totalBuckets: Int): Key

  final override def getBucketBoundary(bucketNum: Int, totalBuckets: Int): PactRecord = {

    val key = getBucketUpperBound(bucketNum, totalBuckets)
    val record = new PactRecord

    val udt = implicitly[UDT[Key]]
    val serializer = udt.getSerializer((0 until udt.fieldTypes.length).toArray)
    serializer.serialize(key, record)

    record
  }
}

