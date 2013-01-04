package eu.stratosphere.pact4s.common.analysis

import scala.collection.mutable
import scala.reflect.Code

abstract class UDF[R: UDT] extends Serializable {

  val outputUDT = implicitly[UDT[R]]
  val outputFields = FieldSet.newOutputSet[R]()

  def getOutputSerializer = outputUDT.getSerializer(outputFields.toSerializerIndexArray)

  def allocateOutputGlobalIndexes(startPos: Int): Int = {

    outputFields.setGlobalized()

    outputFields.map(_.globalPos).foldLeft(startPos) {
      case (i, globalPos @ GlobalPos.Unknown()) => globalPos.setIndex(i); i + 1
      case (i, _)                               => i
    }
  }

  def assignOutputGlobalIndexes(sameAs: FieldSet[Field]): Unit = {

    outputFields.setGlobalized()

    outputFields.foreach {
      case OutputField(localPos, globalPos) => globalPos.setReference(sameAs(localPos).globalPos)
    }
  }

  def setOutputGlobalIndexes(startPos: Int, sameAs: Option[FieldSet[Field]]): Int = sameAs match {
    case None         => allocateOutputGlobalIndexes(startPos)
    case Some(sameAs) => assignOutputGlobalIndexes(sameAs); startPos
  }

  def attachOutputsToInputs(inputFields: FieldSet[InputField]): Unit = {

    inputFields.setGlobalized()

    inputFields.foreach {
      case InputField(localPos, globalPos) => globalPos.setReference(outputFields(localPos).globalPos)
    }
  }

  protected def markFieldCopied(inputGlobalPos: GlobalPos, outputLocalPos: Int): Unit = {
    val outputField = outputFields(outputLocalPos)
    outputField.globalPos.setReference(inputGlobalPos)
    outputField.isUsed = false
  }
}

class UDF0[R: UDT] extends UDF[R]

class UDF1[T: UDT, R: UDT] extends UDF[R] {

  val inputUDT = implicitly[UDT[T]]
  val inputFields = FieldSet.newInputSet[T]()
  val forwardSet = mutable.Set[GlobalPos]()
  val discardSet = mutable.Set[GlobalPos]()
  
  def getInputDeserializer = inputUDT.getSerializer(inputFields.toSerializerIndexArray)
  def getForwardIndexArray = forwardSet.map(_.getValue).toArray
  def getDiscardIndexArray = discardSet.map(_.getValue).toArray

  def markInputFieldUnread(localPos: Int): Unit = {
    inputFields(localPos).isUsed = false
  }

  def markFieldCopied(inputLocalPos: Int, outputLocalPos: Int): Unit = {
    val inputGlobalPos = inputFields(inputLocalPos).globalPos
    forwardSet.add(inputGlobalPos)
    markFieldCopied(inputGlobalPos, outputLocalPos)
  }
}

class UDF2[T1: UDT, T2: UDT, R: UDT] extends UDF[R] {

  val leftInputUDT = implicitly[UDT[T1]]
  val leftInputFields = FieldSet.newInputSet[T1]()
  val leftForwardSet = mutable.Set[GlobalPos]()
  val leftDiscardSet = mutable.Set[GlobalPos]()

  val rightInputUDT = implicitly[UDT[T2]]
  val rightInputFields = FieldSet.newInputSet[T2]()
  val rightForwardSet = mutable.Set[GlobalPos]()
  val rightDiscardSet = mutable.Set[GlobalPos]()

  def getLeftInputDeserializer = leftInputUDT.getSerializer(leftInputFields.toSerializerIndexArray)
  def getLeftForwardIndexArray = leftForwardSet.map(_.getValue).toArray
  def getLeftDiscardIndexArray = leftDiscardSet.map(_.getValue).toArray
  
  def getRightInputDeserializer = rightInputUDT.getSerializer(rightInputFields.toSerializerIndexArray)
  def getRightForwardIndexArray = rightForwardSet.map(_.getValue).toArray
  def getRightDiscardIndexArray = rightDiscardSet.map(_.getValue).toArray
  
  private def getInputField(localPos: Either[Int, Int]): InputField = localPos match {
    case Left(pos)  => leftInputFields(pos)
    case Right(pos) => rightInputFields(pos)
  }

  def markInputFieldUnread(localPos: Either[Int, Int]): Unit = {
    localPos.fold(leftInputFields(_), rightInputFields(_)).isUsed = false
  }

  def markFieldCopied(inputLocalPos: Either[Int, Int], outputLocalPos: Int): Unit = {
    val (fields, forwardSet) = inputLocalPos.fold(_ => (leftInputFields, leftForwardSet), _ => (rightInputFields, rightForwardSet))
    val inputGlobalPos = fields(inputLocalPos.merge).globalPos
    forwardSet.add(inputGlobalPos)
    markFieldCopied(inputGlobalPos, outputLocalPos)
  }
}

