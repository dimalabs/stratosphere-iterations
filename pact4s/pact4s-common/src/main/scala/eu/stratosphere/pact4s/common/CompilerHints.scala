package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.pact.common.contract.DataDistribution
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.util.FieldSet

trait Hintable[T] {

  var hints: Seq[CompilerHint[T]] = null

  def getHints = hints

  def getPactName(default: String) = getHints find { case _: PactName => true } match {
    case Some(PactName(pactName)) => pactName
    case None                     => default
  }

  def applyHints(contract: Contract) = {
    for (hint <- getHints) hint.applyToContract(contract)
  }

  def getGenericHints = getHints filter { classOf[CompilerHint[Nothing]].isInstance(_) } map { _.asInstanceOf[CompilerHint[Nothing]] }
}

abstract class CompilerHint[+T] {
  def applyToContract(contract: Contract)
}

object CompilerHint {
  implicit def hint2SeqHint[T](h: CompilerHint[T]): Seq[CompilerHint[T]] = Seq(h)
}

case class PactName(val pactName: String) extends CompilerHint[Nothing] {
  override def applyToContract(contract: Contract) = {}
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

case class UniqueKey[T: UDT, Key, KeySelector: SelectorBuilder[T, Key]#Selector](val keySelector: T => Key) extends CompilerHint[T] {

  override def applyToContract(contract: Contract) = {

    val fieldSet = new FieldSet(implicitly[FieldSelector[T => Key]].getFields)
    val hints = contract.getCompilerHints()

    val fieldSets = hints.getUniqueFields()
    if (fieldSets == null)
      hints.setUniqueField(fieldSet)
    else
      fieldSets.add(fieldSet)
  }
}

case class KeyCardinality[T: UDT, Key, KeySelector: SelectorBuilder[T, Key]#Selector](val keySelector: T => Key, val numDistinctKeys: Long, val avgNumRecordsPerKey: Option[Long] = None) extends CompilerHint[T] {

  def this(keySelector: T => Key, numDistinctKeys: Long, avgNumRecordsPerKey: Long) = this(keySelector, numDistinctKeys, Some(avgNumRecordsPerKey))

  override def applyToContract(contract: Contract) = {

    val fieldSet = new FieldSet(implicitly[FieldSelector[T => Key]].getFields)
    val hints = contract.getCompilerHints()
    hints.setDistinctCount(fieldSet, numDistinctKeys)

    if (avgNumRecordsPerKey.isDefined)
      hints.setAvgNumRecordsPerDistinctFields(fieldSet, avgNumRecordsPerKey.get)
  }
}

case class InputDistribution(dataDistribution: Class[_ <: InputDataDistribution[_]]) extends CompilerHint[Nothing] {
  override def applyToContract(contract: Contract) = contract.getCompilerHints().setInputDistributionClass(dataDistribution)
}

abstract class InputDataDistribution[Key] extends DataDistribution {

  val udt: UDT[Key]
  def getSplitBorder(splitId: Int, totalSplits: Int): Key

  final override def getSplit(splitId: Int, totalSplits: Int): PactRecord = {

    val key = getSplitBorder(splitId, totalSplits)
    val record = new PactRecord

    val serializer = udt.createSerializer((0 until udt.fieldTypes.length).toArray)
    serializer.serialize(key, record)

    record
  }
}
