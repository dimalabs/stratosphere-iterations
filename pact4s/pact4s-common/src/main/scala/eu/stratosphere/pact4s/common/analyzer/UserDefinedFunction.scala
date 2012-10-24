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

import scala.reflect.Code

abstract sealed class AmbientFieldBehavior

object AmbientFieldBehavior {
  case object Default extends AmbientFieldBehavior
  case object Forward extends AmbientFieldBehavior
  case object Discard extends AmbientFieldBehavior
}

trait UDF extends Serializable {

  def isGlobalized: Boolean
  def getWriteFields: Array[Int]
  def getOutputFields: Map[Int, Int]

  def relocateInputField(oldPosition: Int, newPosition: Int)
  def discardUnusedOutputFields(used: Set[Int])
}

trait UDF1Code[T] extends UDF.EmptyCode[T] with UDF1 { def userFunction: T }
trait UDF2Code[T] extends UDF.EmptyCode[T] with UDF2 { def userFunction: T }

trait UDFLowPriorityImplicits {

  class UDFAnalysisFailedException extends RuntimeException("UDF analysis failed. This should have been caught at compile time.")

  implicit def unanalyzedUDF1[T1, R](fun: T1 => R): UDF1Code[T1 => R] = throw new UDFAnalysisFailedException
  implicit def unanalyzedUDF2[T1, T2, R](fun: (T1, T2) => R): UDF2Code[(T1, T2) => R] = throw new UDFAnalysisFailedException
  implicit def unanalyzedUDF1Code[T1, R](fun: Code[T1 => R]): UDF1Code[T1 => R] = throw new UDFAnalysisFailedException
  implicit def unanalyzedUDF2Code[T1, T2, R](fun: Code[(T1, T2) => R]): UDF2Code[(T1, T2) => R] = throw new UDFAnalysisFailedException
}

object UDF extends UDFLowPriorityImplicits {

  abstract class EmptyCode[T] extends Code[T](null) {
    @transient override val tree = null
  }
}

trait UDF1 extends UDF {

  def getReadFields: Array[Int]
  def getCopiedFields: Map[Int, Int]
  def getForwardedFields: Array[Int]
  def getDiscardedFields: Array[Int]

  def copy(): UDF1
  def markInputFieldUnread(inputFieldNum: Int)
  def markInputFieldCopied(fromInputFieldNum: Int, toOutputFieldNum: Int)

  protected def globalize(inputLocations: Map[Int, Int], outputLocation: Int): Int
  protected def globalize(inputLocations: Map[Int, Int], outputLocations: Map[Int, Int])

  def globalize(inputLocations: Map[Int, Int], freePos: Int, outputLocations: Option[Map[Int, Int]]): Int = outputLocations match {
    case Some(outputLocations) => { globalize(inputLocations, outputLocations); freePos }
    case None                  => globalize(inputLocations, freePos)
  }

  def setAmbientFieldBehavior(position: Int, behavior: AmbientFieldBehavior)
}

trait UDF2 extends UDF {

  def getReadFields: (Array[Int], Array[Int])
  def getCopiedFields: Map[Int, Either[Int, Int]]
  def getForwardedFields: (Array[Int], Array[Int])
  def getDiscardedFields: (Array[Int], Array[Int])

  def getAllForwardedFields = {
    val (left, right) = getForwardedFields
    (left ++ right) toArray
  }

  def getAllDiscardedFields = {
    val (left, right) = getDiscardedFields
    (left ++ right) toArray
  }

  def markInputFieldUnread(inputFieldNum: Either[Int, Int])
  def markInputFieldCopied(fromInputFieldNum: Either[Int, Int], toOutputFieldNum: Int)

  protected def globalize(leftInputLocations: Map[Int, Int], rightInputLocations: Map[Int, Int], outputLocation: Int): Int
  protected def globalize(leftInputLocations: Map[Int, Int], rightInputLocations: Map[Int, Int], outputLocations: Map[Int, Int])

  def globalize(leftInputLocations: Map[Int, Int], rightInputLocations: Map[Int, Int], freePos: Int, outputLocations: Option[Map[Int, Int]]): Int = outputLocations match {
    case Some(outputLocations) => { globalize(leftInputLocations, rightInputLocations, outputLocations); freePos }
    case None                  => globalize(leftInputLocations, rightInputLocations, freePos)
  }

  def setAmbientFieldBehavior(position: Either[Int, Int], behavior: AmbientFieldBehavior)
}

