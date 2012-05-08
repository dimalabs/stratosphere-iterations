package eu.stratosphere.pact4s.common.analyzer

import scala.collection.mutable

abstract class AnalyzedUDF(outputLength: Int) extends UDF {

  private var outputLocation: Option[Int] = None
  private val writeFields = createIdentityMap(outputLength)

  override def isGlobalized = outputLocation.isDefined
  override def getWriteFields = writeFields
  override def getOutputLocation = { assertGlobalized(true); outputLocation.get }

  protected def getReadFieldSets: Seq[Array[Int]]

  protected def createIdentityMap(length: Int) = (0 until length).toArray

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != isGlobalized)
      throw new IllegalStateException("ExpectGlobalized: " + expectGlobalized + ", Globalized: " + isGlobalized + ", OutputLocation: " + outputLocation)
  }

  protected def globalize(inputLocations: Seq[Map[Int, Int]], outputLocation: Int): Int = {
    assertGlobalized(false)

    for ((readFields, inputLocations) <- getReadFieldSets zip inputLocations) {
      for (fieldNum <- 0 until readFields.length if readFields(fieldNum) >= 0) {
        readFields(fieldNum) = inputLocations(fieldNum)
      }
    }

    if (outputLocation >= 0) {

      for (fieldNum <- 0 until writeFields.length if writeFields(fieldNum) >= 0)
        writeFields(fieldNum) += outputLocation

    } else {

      if (inputLocations.size > 1)
        throw new IllegalArgumentException("Attempted to perform in-place globalization on a Function2")

      for (fieldNum <- 0 until writeFields.length if writeFields(fieldNum) >= 0)
        writeFields(fieldNum) = inputLocations.head(fieldNum)
    }

    this.outputLocation = Some(outputLocation)
    outputLocation + writeFields.length
  }

  protected def markInputFieldUnused(readFields: Array[Int], inputFieldNum: Int) = {
    assertGlobalized(false)
    readFields(inputFieldNum) = -1
  }

  protected def markOutputFieldUnused(outputFieldNum: Int) = {
    assertGlobalized(false)
    writeFields(outputFieldNum) = -1
  }

  protected def markOutputFieldUsed(outputFieldNum: Int) = {
    assertGlobalized(true)
    writeFields(outputFieldNum) = getOutputLocation + outputFieldNum
  }

  protected def assertAmbience(outputPosition: Int) = {
    assertGlobalized(true)
    if (writeFields.contains(outputPosition))
      throw new IllegalArgumentException("Field is not ambient.")
  }

  override def relocateInputField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    for (readFields <- getReadFieldSets) {
      for (fieldNum <- 0 until readFields.length if readFields(fieldNum) == oldPosition) {
        readFields(fieldNum) = newPosition
      }
    }
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

  override def getOutputFields: Map[Int, Int] = {
    assertGlobalized(true)

    getWriteFields.zipWithIndex.map {

      case (-1, fieldNum)  => (fieldNum, copiedFields(fieldNum))
      case (pos, fieldNum) => (fieldNum, pos)
    } toMap
  }

  def copy(): AnalyzedUDF1[T1, R] = {
    assertGlobalized(false)

    val udf = new AnalyzedUDF1[T1, R](inputLength, outputLength)

    for (field <- 0 until inputLength if readFields(field) < 0)
      udf.markInputFieldUnread(field)

    for ((to, from) <- copiedFields)
      udf.markInputFieldCopied(from, to)

    udf
  }

  override def markInputFieldUnread(inputFieldNum: Int) = {

    markInputFieldUnused(readFields, inputFieldNum)
  }

  override def markInputFieldCopied(fromInputFieldNum: Int, toOutputFieldNum: Int) = {

    markOutputFieldUnused(toOutputFieldNum)
    copiedFields(toOutputFieldNum) = fromInputFieldNum
  }

  override def globalize(inputLocations: Map[Int, Int], outputLocation: Int): Int = {

    var freePos = globalize(Seq(inputLocations), outputLocation)

    copiedFields = copiedFields map {
      case (to, from) => (to, inputLocations(from))
    }

    for (pos <- copiedFields.values)
      ambientFields(pos) = true

    freePos
  }

  override def globalizeInPlace(inputLocations: Map[Int, Int]) = {

    globalize(Seq(inputLocations), -1)

    copiedFields = copiedFields map {
      case (to, from) => (to, inputLocations(from))
    }

    for (pos <- copiedFields.values)
      ambientFields(pos) = true
  }

  override def setAmbientFieldBehavior(position: Int, behavior: AmbientFieldBehavior) = {
    assertAmbience(position)

    behavior match {

      case AmbientFieldBehavior.Default => ambientFields.remove(position)
      case AmbientFieldBehavior.Forward => ambientFields(position) = true
      case AmbientFieldBehavior.Discard => {

        for ((lPos, gPos) <- getOutputFields if gPos == position)
          markOutputFieldUsed(lPos)

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

  override def getOutputFields: Map[Int, Int] = {
    assertGlobalized(true)

    getWriteFields.zipWithIndex.map {

      case (-1, fieldNum)  => (fieldNum, copiedFields(fieldNum).fold(identity, identity))
      case (pos, fieldNum) => (fieldNum, pos)
    } toMap
  }

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

  override def globalize(leftInputLocations: Map[Int, Int], rightInputLocations: Map[Int, Int], outputLocation: Int): Int = {

    var freePos = globalize(Seq(leftInputLocations, rightInputLocations), outputLocation)

    copiedFields = copiedFields map {
      case (to, Left(from))  => (to, Left(leftInputLocations(from)))
      case (to, Right(from)) => (to, Right(rightInputLocations(from)))
    }

    freePos
  }

  override def setAmbientFieldBehavior(position: Either[Int, Int], behavior: AmbientFieldBehavior) = {
    val universalPos = position.fold(identity, identity)
    assertAmbience(universalPos)

    behavior match {

      case AmbientFieldBehavior.Default => ambientFields.remove(position)
      case AmbientFieldBehavior.Forward => ambientFields(position) = true
      case AmbientFieldBehavior.Discard => {

        for ((lPos, gPos) <- getOutputFields if gPos == universalPos)
          markOutputFieldUsed(lPos)

        ambientFields(position) = false
      }
    }
  }

  private def split(items: Iterable[Either[Int, Int]]) = {
    val (left, right) = items.partition(_.isLeft)
    (left map (_.fold(identity, identity)) toArray, right map (_.fold(identity, identity)) toArray)
  }
}

