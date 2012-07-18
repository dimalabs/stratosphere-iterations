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

import scala.collection.mutable

abstract class AnalyzedUDF(outputLength: Int) extends UDF {

  private case class WriteField(val pos: Int, val isCopy: Boolean, val discard: Boolean)

  private var globalized = false
  private val writeFields = createIdentityMap(outputLength) map { WriteField(_, false, false) }

  override def isGlobalized = globalized

  override def getWriteFields = writeFields map {
    case WriteField(pos, false, false) => pos
    case _                             => -1
  }

  protected def getReadFieldSets: Seq[Array[Int]]

  protected def createIdentityMap(length: Int) = (0 until length).toArray

  protected def assertGlobalized(expectGlobalized: Boolean) = {
    if (expectGlobalized != isGlobalized)
      throw new IllegalStateException("ExpectGlobalized: " + expectGlobalized + ", Globalized: " + isGlobalized)
  }

  protected def globalize(inputLocations: Seq[Map[Int, Int]], outputLocation: Int): Int = {

    globalize(inputLocations, writeFields.zipWithIndex map { case (WriteField(pos, _, _), fieldNum) => (fieldNum, pos + outputLocation) } toMap)
    outputLocation + writeFields.length
  }

  protected def globalize(inputLocations: Seq[Map[Int, Int]], outputLocations: Map[Int, Int]) = {
    assertGlobalized(false)

    for ((readFields, inputLocations) <- getReadFieldSets zip inputLocations) {
      for ((pos, fieldNum) <- readFields.zipWithIndex if pos >= 0) {
        readFields(fieldNum) = inputLocations(fieldNum)
      }
    }

    for ((WriteField(pos, isCopy, discard), fieldNum) <- writeFields.zipWithIndex)
      writeFields(fieldNum) = WriteField(outputLocations(fieldNum), isCopy, discard)

    globalized = true
  }

  protected def markInputFieldUnused(readFields: Array[Int], inputFieldNum: Int) = {
    assertGlobalized(false)
    readFields(inputFieldNum) = -1
  }

  protected def markOutputFieldCopied(outputFieldNum: Int) = {
    writeFields(outputFieldNum) = writeFields(outputFieldNum).copy(isCopy = true)
  }

  protected def markOutputFieldWritten(outputFieldNum: Int) = {
    assertGlobalized(true)
    writeFields(outputFieldNum) = writeFields(outputFieldNum).copy(isCopy = false)
  }

  protected def markOutputFieldUnused(outputFieldNum: Int) = {
    assertGlobalized(true)
    writeFields(outputFieldNum) = writeFields(outputFieldNum).copy(discard = true)
  }

  protected def markOutputFieldUsed(outputFieldNum: Int) = {
    assertGlobalized(true)
    writeFields(outputFieldNum) = writeFields(outputFieldNum).copy(discard = false)
  }

  protected def assertAmbience(outputPosition: Int) = {
    assertGlobalized(true)

    if (writeFields.exists(_.pos == outputPosition))
      throw new IllegalArgumentException("Field is not ambient.")
  }

  override def relocateInputField(oldPosition: Int, newPosition: Int) = {
    assertGlobalized(true)

    for (readFields <- getReadFieldSets) {
      for ((pos, fieldNum) <- readFields.zipWithIndex if pos == oldPosition) {
        readFields(fieldNum) = newPosition
      }
    }
  }
}

class AnalyzedUDF1[T1, R](inputLength: Int, outputLength: Int) extends AnalyzedUDF(outputLength) with UDF1[T1 => R] {

  private val readFields = createIdentityMap(inputLength)
  private val copiedFields = mutable.Map[Int, Int]()
  private val ambientFields = mutable.Map[Int, Boolean]()

  override def getReadFields = readFields
  override def getReadFieldSets = Seq(readFields)
  override def getCopiedFields = copiedFields toMap
  override def getForwardedFields = (ambientFields filter { case (_, forward) => forward } keys) toArray
  override def getDiscardedFields = (ambientFields filterNot { case (_, forward) => forward } keys) toArray

  override def getOutputFields: Map[Int, Int] = {
    assertGlobalized(true)

    getWriteFields.zipWithIndex.map {

      case (-1, fieldNum)  => (fieldNum, copiedFields.getOrElse(fieldNum, -1))
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

    markOutputFieldCopied(toOutputFieldNum)
    copiedFields(toOutputFieldNum) = fromInputFieldNum
  }

  override def globalize(inputLocations: Map[Int, Int], outputLocation: Int): Int = {

    var freePos = globalize(Seq(inputLocations), outputLocation)

    for ((to, from) <- copiedFields) {
      copiedFields(to) = inputLocations(from)
      ambientFields(from) = true
    }

    freePos
  }

  override def globalize(inputLocations: Map[Int, Int], outputLocations: Map[Int, Int]) = {

    globalize(Seq(inputLocations), outputLocations)

    for ((to, from) <- copiedFields) {

      if (inputLocations(from) == outputLocations(to)) {
        copiedFields(to) = inputLocations(from)
        ambientFields(from) = true
      } else {
        copiedFields.remove(to)
        markOutputFieldWritten(to)
      }
    }
  }

  override def setAmbientFieldBehavior(position: Int, behavior: AmbientFieldBehavior) = {
    assertAmbience(position)

    for ((to, from) <- copiedFields if from == position) {
      behavior match {
        case AmbientFieldBehavior.Default | AmbientFieldBehavior.Forward => markOutputFieldCopied(to)
        case AmbientFieldBehavior.Discard                                => markOutputFieldWritten(to)
      }
    }

    behavior match {
      case AmbientFieldBehavior.Default => ambientFields.remove(position)
      case AmbientFieldBehavior.Forward => ambientFields(position) = true
      case AmbientFieldBehavior.Discard => ambientFields(position) = false
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

      case (-1, fieldNum)  => (fieldNum, copiedFields.getOrElse(fieldNum, Left(-1)).fold(identity, identity))
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

    markOutputFieldCopied(toOutputFieldNum)
    copiedFields(toOutputFieldNum) = fromInputFieldNum
  }

  override def globalize(leftInputLocations: Map[Int, Int], rightInputLocations: Map[Int, Int], outputLocation: Int): Int = {

    var freePos = globalize(Seq(leftInputLocations, rightInputLocations), outputLocation)

    for ((to, from) <- copiedFields) {

      copiedFields(to) = from.fold(from => Left(leftInputLocations(from)), from => Right(rightInputLocations(from)))
      ambientFields(from) = true
    }

    freePos
  }

  override def globalize(leftInputLocations: Map[Int, Int], rightInputLocations: Map[Int, Int], outputLocations: Map[Int, Int]) = {

    globalize(Seq(leftInputLocations, rightInputLocations), outputLocations)

    for ((to, from) <- copiedFields) {

      val universalPos = from.fold({ leftInputLocations(_) }, { rightInputLocations(_) })

      if (universalPos == outputLocations(to)) {
        copiedFields(to) = from.fold(from => Left(leftInputLocations(from)), from => Right(rightInputLocations(from)))
        ambientFields(from) = true
      } else {
        copiedFields.remove(to)
        markOutputFieldWritten(to)
      }
    }
  }

  override def setAmbientFieldBehavior(position: Either[Int, Int], behavior: AmbientFieldBehavior) = {
    assertAmbience(position.fold(identity, identity))

    for ((to, from) <- copiedFields if from == position) {
      behavior match {
        case AmbientFieldBehavior.Default | AmbientFieldBehavior.Forward => markOutputFieldCopied(to)
        case AmbientFieldBehavior.Discard                                => markOutputFieldWritten(to)
      }
    }

    behavior match {

      case AmbientFieldBehavior.Default => ambientFields.remove(position)
      case AmbientFieldBehavior.Forward => ambientFields(position) = true
      case AmbientFieldBehavior.Discard => ambientFields(position) = false
    }
  }

  private def split(items: Iterable[Either[Int, Int]]) = {
    val (left, right) = items.partition(_.isLeft)
    (left map (_.fold(identity, identity)) toArray, right map (_.fold(identity, identity)) toArray)
  }
}

