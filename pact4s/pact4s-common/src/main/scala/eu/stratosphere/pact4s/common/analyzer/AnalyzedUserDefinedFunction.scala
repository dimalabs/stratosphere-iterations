package eu.stratosphere.pact4s.common.analyzer

import scala.collection.mutable

abstract class AnalyzedUDF(outputLength: Int) extends UDF {

  private var globalized = false
  private val writeFields = createIdentityMap(outputLength)
  private val discardedWriteFields = mutable.Map[Int, Int]()

  override def isGlobalized = globalized
  override def getWriteFields = writeFields

  protected def getReadFieldSets: Seq[Array[Int]]

  protected def createIdentityMap(length: Int) = (0 until length).toArray

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != globalized)
      throw new IllegalStateException()
  }

  protected def globalize(inputLocations: Seq[Int], outputLocation: Int) = {
    assertGlobalized(false)

    for ((readFields, inputLocation) <- getReadFieldSets zip inputLocations) {
      for (fieldNum <- 0 to readFields.length if readFields(fieldNum) > -1) {
        readFields(fieldNum) += inputLocation
      }
    }

    for (fieldNum <- 0 to writeFields.length if writeFields(fieldNum) > -1) {
      writeFields(fieldNum) += outputLocation
    }

    globalized = true
  }

  protected def markInputFieldUnused(readFields: Array[Int], inputFieldNum: Int) = {
    assertGlobalized(false)
    readFields(inputFieldNum) = -1
  }

  protected def markOutputFieldUnused(outputFieldNum: Int) = {
    assertGlobalized(false)
    writeFields(outputFieldNum) = -1
  }

  override def relocateInputField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    for (readFields <- getReadFieldSets) {
      for (fieldNum <- 0 to readFields.length if readFields(fieldNum) == oldPosition) {
        readFields(fieldNum) = newPosition
      }
    }
  }

  protected def discardOutputField(position: Int) = {
    assertGlobalized(true)

    val outputFieldNum = getOutputFieldNumber(position)

    if (outputFieldNum >= 0) {
      discardedWriteFields(position) = outputFieldNum
      writeFields(outputFieldNum) = -1
      true
    } else {
      false
    }
  }

  protected def restoreOutputField(position: Int): Boolean = {
    assertGlobalized(true)

    val outputFieldNum = getOutputFieldNumber(position)

    if (outputFieldNum >= 0) {
      discardedWriteFields.remove(position)
      writeFields(outputFieldNum) = position
      true
    } else {
      false
    }
  }

  private def getOutputFieldNumber(position: Int) = {
    discardedWriteFields.getOrElse(position, writeFields indexOf position)
  }
}

class AnalyzedUDF1[T1, R](inputLength: Int, outputLength: Int) extends AnalyzedUDF(outputLength) with UDF1[T1 => R] {

  private val readFields = createIdentityMap(inputLength)
  private var copiedFields = mutable.Map[Int, Int]()
  private val ambientFields = mutable.Map[Int, Boolean]()

  override def getReadFields = readFields
  override def getReadFieldSets = Seq(readFields)
  override def getCopiedFields = copiedFields toMap
  override def getForwardedFields = (ambientFields filter { case (_, forward) => forward } keys) toArray
  override def getDiscardedFields = (ambientFields filterNot { case (_, forward) => forward } keys) toArray

  override def markInputFieldUnread(inputFieldNum: Int) = {

    markInputFieldUnused(readFields, inputFieldNum)
  }

  override def markInputFieldCopied(fromInputFieldNum: Int, toOutputFieldNum: Int) = {

    markOutputFieldUnused(toOutputFieldNum)
    copiedFields(toOutputFieldNum) = fromInputFieldNum
  }

  override def globalize(inputLocation: Int, outputLocation: Int) = {

    globalize(Seq(inputLocation), outputLocation)

    copiedFields = copiedFields map {
      case (to, from) => (to + outputLocation, from + inputLocation)
    }
  }

  override def setAmbientFieldBehavior(position: Int, behavior: AmbientFieldBehavior) = behavior match {

    case AmbientFieldBehavior.Default => {
      if (!restoreOutputField(position)) {
        ambientFields.remove(position)
      }
    }

    case AmbientFieldBehavior.Forward => {
      if (!restoreOutputField(position)) {
        ambientFields(position) = true
      }
    }

    case AmbientFieldBehavior.Discard => {
      if (!discardOutputField(position)) {
        ambientFields(position) = false
      }
    }
  }
}

class AnalyzedUDF2[T1, T2, R](leftInputLength: Int, rightInputLength: Int, outputLength: Int) extends AnalyzedUDF(outputLength) with UDF2[(T1, T2) => R] {

  private val leftReadFields = createIdentityMap(leftInputLength)
  private val rightReadFields = createIdentityMap(rightInputLength)
  private var copiedFields = mutable.Map[Int, Either[Int, Int]]()
  private val ambientFields = mutable.Map[Either[Int, Int], Boolean]()

  override def getReadFields = (leftReadFields, rightReadFields)
  override def getReadFieldSets = Seq(leftReadFields, rightReadFields)
  override def getCopiedFields = copiedFields toMap
  override def getForwardedFields = split(ambientFields filter { case (_, forward) => forward } keys)
  override def getDiscardedFields = split(ambientFields filterNot { case (_, forward) => forward } keys)

  override def markInputFieldUnread(inputFieldNum: Either[Int, Int]) = {

    inputFieldNum match {
      case Left(inputFieldNum)  => { markInputFieldUnused(leftReadFields, inputFieldNum) }
      case Right(inputFieldNum) => { markInputFieldUnused(rightReadFields, inputFieldNum) }
    }
  }

  override def markInputFieldCopied(fromInputFieldNum: Either[Int, Int], toOutputFieldNum: Int) = {

    markOutputFieldUnused(toOutputFieldNum)
    copiedFields(toOutputFieldNum) = fromInputFieldNum
  }

  override def globalize(leftInputLocation: Int, rightInputLocation: Int, outputLocation: Int) = {

    globalize(Seq(leftInputLocation, rightInputLocation), outputLocation)

    copiedFields = copiedFields map {
      case (to, Left(from))  => (to + outputLocation, Left(from + leftInputLocation))
      case (to, Right(from)) => (to + outputLocation, Right(from + rightInputLocation))
    }
  }

  override def setAmbientFieldBehavior(position: Either[Int, Int], behavior: AmbientFieldBehavior) = {

    val pos = unpack(position)

    behavior match {

      case AmbientFieldBehavior.Default => {
        if (!restoreOutputField(pos)) {
          ambientFields.remove(position)
        }
      }

      case AmbientFieldBehavior.Forward => {
        if (!restoreOutputField(pos)) {
          ambientFields(position) = true
        }
      }

      case AmbientFieldBehavior.Discard => {
        if (!discardOutputField(pos)) {
          ambientFields(position) = false
        }
      }
    }
  }

  private def unpack(item: Either[Int, Int]) = item match {
    case Left(x)  => x
    case Right(x) => x
  }

  private def split(items: Iterable[Either[Int, Int]]) = {
    val (left, right) = items.partition(_.isLeft)
    (left map unpack toArray, right map unpack toArray)
  }
}

