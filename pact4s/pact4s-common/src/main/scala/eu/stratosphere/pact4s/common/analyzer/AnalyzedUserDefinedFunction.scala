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

trait AnalyzedUDF extends UDF {

  protected case class WriteField(val pos: Int, val isCopy: Boolean, val discard: Option[Boolean])

  private var globalized = false
  protected val writeFields: Array[WriteField]

  protected def getInitialReadFields(inputLength: Int): Array[Int] = createIdentityMap(inputLength)
  protected def getInitialWriteFields(outputLength: Int): Array[WriteField] = createIdentityMap(outputLength) map { WriteField(_, false, None) }

  override def isGlobalized = globalized

  override def getWriteFields = writeFields map {
    case WriteField(pos, false, None)        => pos
    case WriteField(pos, false, Some(false)) => pos
    case _                                   => -1
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

  override def discardUnusedOutputFields(used: Set[Int]) = {

    assertGlobalized(true)

    for ((WriteField(pos, isCopy, discard), fieldNum) <- writeFields.zipWithIndex) {
      val newDiscard = discard match {
        case None | Some(true) => Some(!used.contains(pos))
        case Some(false)       => Some(false)
      }
      writeFields(fieldNum) = WriteField(pos, isCopy, newDiscard)
    }
  }
}

trait AnalyzedUDF1 extends AnalyzedUDF with UDF1 {

  protected val readFields: Array[Int]
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

  def copy(): AnalyzedUDF1 = {
    assertGlobalized(false)

    val inLength = readFields.length
    val outLength = getWriteFields.length
    val udf = new AnalyzedUDF1 {
      override val readFields = getInitialReadFields(inLength)
      override val writeFields = getInitialWriteFields(outLength)
    }

    for (field <- 0 until readFields.length if readFields(field) < 0)
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
  
  override def discardUnusedOutputFields(used: Set[Int]) = {
    super.discardUnusedOutputFields(used)
    
    for ((pos, forward) <- ambientFields if forward && !used.contains(pos))
      setAmbientFieldBehavior(pos, AmbientFieldBehavior.Discard)
  }
}

object AnalyzedUDF1 {

  def default[T1: UDT, R: UDT](fun: T1 => R): UDF1Code[T1 => R] = new UDF1Code[T1 => R] with AnalyzedUDF1 {
    override def userFunction = fun
    override val readFields = getInitialReadFields(implicitly[UDT[T1]].numFields)
    override val writeFields = getInitialWriteFields(implicitly[UDT[R]].numFields)
  }

  def defaultIterT[T1: UDT, R: UDT](fun: Iterator[T1] => R): UDF1Code[Iterator[T1] => R] = new UDF1Code[Iterator[T1] => R] with AnalyzedUDF1 {
    override def userFunction = fun
    override val readFields = getInitialReadFields(implicitly[UDT[T1]].numFields)
    override val writeFields = getInitialWriteFields(implicitly[UDT[R]].numFields)
  }

  def defaultIterR[T1: UDT, R: UDT](fun: T1 => Iterator[R]): UDF1Code[T1 => Iterator[R]] = new UDF1Code[T1 => Iterator[R]] with AnalyzedUDF1 {
    override def userFunction = fun
    override val readFields = getInitialReadFields(implicitly[UDT[T1]].numFields)
    override val writeFields = getInitialWriteFields(implicitly[UDT[R]].numFields)
  }
}

trait AnalyzedUDF2 extends AnalyzedUDF with UDF2 {

  protected val leftReadFields: Array[Int]
  protected val rightReadFields: Array[Int]
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

  override def discardUnusedOutputFields(used: Set[Int]) = {
    super.discardUnusedOutputFields(used)
    
    for ((pos, forward) <- ambientFields if forward && !used.contains(pos.fold(identity, identity)))
      setAmbientFieldBehavior(pos, AmbientFieldBehavior.Discard)
  }

  private def split(items: Iterable[Either[Int, Int]]) = {
    val (left, right) = items.partition(_.isLeft)
    (left map (_.fold(identity, identity)) toArray, right map (_.fold(identity, identity)) toArray)
  }
}

object AnalyzedUDF2 {

  def default[T1: UDT, T2: UDT, R: UDT](fun: (T1, T2) => R): UDF2Code[(T1, T2) => R] = new UDF2Code[(T1, T2) => R] with AnalyzedUDF2 {
    override def userFunction = fun
    override val leftReadFields = getInitialReadFields(implicitly[UDT[T1]].numFields)
    override val rightReadFields = getInitialReadFields(implicitly[UDT[T2]].numFields)
    override val writeFields = getInitialWriteFields(implicitly[UDT[R]].numFields)
  }

  def defaultIterR[T1: UDT, T2: UDT, R: UDT](fun: (T1, T2) => Iterator[R]): UDF2Code[(T1, T2) => Iterator[R]] = new UDF2Code[(T1, T2) => Iterator[R]] with AnalyzedUDF2 {
    override def userFunction = fun
    override val leftReadFields = getInitialReadFields(implicitly[UDT[T1]].numFields)
    override val rightReadFields = getInitialReadFields(implicitly[UDT[T2]].numFields)
    override val writeFields = getInitialWriteFields(implicitly[UDT[R]].numFields)
  }

  def defaultIterT[T1: UDT, T2: UDT, R: UDT](fun: (Iterator[T1], Iterator[T2]) => R): UDF2Code[(Iterator[T1], Iterator[T2]) => R] = new UDF2Code[(Iterator[T1], Iterator[T2]) => R] with AnalyzedUDF2 {
    override def userFunction = fun
    override val leftReadFields = getInitialReadFields(implicitly[UDT[T1]].numFields)
    override val rightReadFields = getInitialReadFields(implicitly[UDT[T2]].numFields)
    override val writeFields = getInitialWriteFields(implicitly[UDT[R]].numFields)
  }

  def defaultIterTR[T1: UDT, T2: UDT, R: UDT](fun: (Iterator[T1], Iterator[T2]) => Iterator[R]): UDF2Code[(Iterator[T1], Iterator[T2]) => Iterator[R]] = new UDF2Code[(Iterator[T1], Iterator[T2]) => Iterator[R]] with AnalyzedUDF2 {
    override def userFunction = fun
    override val leftReadFields = getInitialReadFields(implicitly[UDT[T1]].numFields)
    override val rightReadFields = getInitialReadFields(implicitly[UDT[T2]].numFields)
    override val writeFields = getInitialWriteFields(implicitly[UDT[R]].numFields)
  }
}
