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

package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analysis._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class CrossParameters[LeftIn, RightIn, Out](
  val leftDeserializer: UDTSerializer[LeftIn],
  val leftDiscard: Array[Int],
  val rightDeserializer: UDTSerializer[RightIn],
  val rightForward: Array[Int],
  val serializer: UDTSerializer[Out],
  val outputLength: Int,
  val userFunction: CrossParameters.FunType[LeftIn, RightIn, Out])
  extends StubParameters

object CrossParameters {
  
  type FunType[LeftIn, RightIn, Out] = Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]
  
  def getStubFor[LeftIn, RightIn, Out](mapFunction: FunType[LeftIn, RightIn, Out]) = mapFunction match {
    case Left(_)  => classOf[Cross4sStub[LeftIn, RightIn, Out]]
    case Right(_) => classOf[FlatCross4sStub[LeftIn, RightIn, Out]]
  }
}
  
abstract sealed class Cross4sStubBase[LeftIn, RightIn, Out, R] extends CrossStub {
  
  protected var leftDeserializer: UDTSerializer[LeftIn] = _
  protected var leftDiscard: Array[Int] = _
  protected var rightDeserializer: UDTSerializer[RightIn] = _
  protected var rightForward: Array[Int] = _
  protected var serializer: UDTSerializer[Out] = _
  protected var outputLength: Int = _
  protected var userFunction: (LeftIn, RightIn) => R = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[CrossParameters[LeftIn, RightIn, Out]](config)

    this.leftDeserializer = parameters.leftDeserializer
    this.leftDiscard = parameters.leftDiscard
    this.rightDeserializer = parameters.rightDeserializer
    this.rightForward = parameters.rightForward
    this.serializer = parameters.serializer
    this.outputLength = parameters.outputLength
    this.userFunction = getUserFunction(parameters)
  }

  protected def getUserFunction(parameters: CrossParameters[LeftIn, RightIn, Out]): (LeftIn, RightIn) => R
}

final class Cross4sStub[LeftIn, RightIn, Out] extends Cross4sStubBase[LeftIn, RightIn, Out, Out] {

  override def getUserFunction(parameters: CrossParameters[LeftIn, RightIn, Out]) = parameters.userFunction.left.get

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = userFunction.apply(left, right)

    leftRecord.setNumFields(outputLength)

    for (field <- leftDiscard)
      leftRecord.setNull(field)

    leftRecord.copyFrom(rightRecord, rightForward, rightForward)

    serializer.serialize(output, leftRecord)
    out.collect(leftRecord)
  }
}

final class FlatCross4sStub[LeftIn, RightIn, Out] extends Cross4sStubBase[LeftIn, RightIn, Out, Iterator[Out]] {

  override def getUserFunction(parameters: CrossParameters[LeftIn, RightIn, Out]) = parameters.userFunction.right.get

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = userFunction.apply(left, right)

    if (output.nonEmpty) {

      leftRecord.setNumFields(outputLength)

      for (field <- leftDiscard)
        leftRecord.setNull(field)

      leftRecord.copyFrom(rightRecord, rightForward, rightForward)

      for (item <- output) {
        serializer.serialize(item, leftRecord)
        out.collect(leftRecord)
      }
    }
  }
}
