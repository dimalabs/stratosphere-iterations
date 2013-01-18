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

import java.util.{ Iterator => JIterator }

import eu.stratosphere.pact4s.common.analysis._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class CoGroupParameters[LeftIn, RightIn, Out](
  val leftDeserializer: UDTSerializer[LeftIn],
  val leftForward: Array[Int],
  val rightDeserializer: UDTSerializer[RightIn],
  val rightForward: Array[Int],
  val serializer: UDTSerializer[Out],
  val outputLength: Int,
  val userFunction: CoGroupParameters.FunType[LeftIn, RightIn, Out])
  extends StubParameters

object CoGroupParameters {
  
  type FunType[LeftIn, RightIn, Out] = Either[(Iterator[LeftIn], Iterator[RightIn]) => Out, (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]
  
  def getStubFor[LeftIn, RightIn, Out](mapFunction: FunType[LeftIn, RightIn, Out]) = mapFunction match {
    case Left(_)  => classOf[CoGroup4sStub[LeftIn, RightIn, Out]]
    case Right(_) => classOf[FlatCoGroup4sStub[LeftIn, RightIn, Out]]
  }
}

abstract sealed class CoGroup4sStubBase[LeftIn, RightIn, Out, R] extends CoGroupStub {
  
  protected val outputRecord = new PactRecord()

  protected var leftIterator: DeserializingIterator[LeftIn] = _
  protected var leftForward: Array[Int] = _
  protected var rightIterator: DeserializingIterator[RightIn] = _
  protected var rightForward: Array[Int] = _
  protected var serializer: UDTSerializer[Out] = _
  protected var userFunction: (Iterator[LeftIn], Iterator[RightIn]) => R = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[CoGroupParameters[LeftIn, RightIn, Out]](config)

    this.outputRecord.setNumFields(parameters.outputLength)

    this.leftIterator = new DeserializingIterator(parameters.leftDeserializer)
    this.leftForward = parameters.leftForward
    this.rightIterator = new DeserializingIterator(parameters.rightDeserializer)
    this.rightForward = parameters.rightForward
    this.serializer = parameters.serializer
    this.userFunction = getUserFunction(parameters)
  }
  
  protected def getUserFunction(parameters: CoGroupParameters[LeftIn, RightIn, Out]): (Iterator[LeftIn], Iterator[RightIn]) => R
}

final class CoGroup4sStub[LeftIn, RightIn, Out] extends CoGroup4sStubBase[LeftIn, RightIn, Out, Out] {

  override def getUserFunction(parameters: CoGroupParameters[LeftIn, RightIn, Out]) = parameters.userFunction.left.get

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector[PactRecord]) = {

    val firstLeftRecord = leftIterator.initialize(leftRecords)
    outputRecord.copyFrom(firstLeftRecord, leftForward, leftForward)

    val firstRightRecord = rightIterator.initialize(rightRecords)
    outputRecord.copyFrom(firstRightRecord, rightForward, rightForward)

    val output = userFunction.apply(leftIterator, rightIterator)

    serializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

final class FlatCoGroup4sStub[LeftIn, RightIn, Out] extends CoGroup4sStubBase[LeftIn, RightIn, Out, Iterator[Out]] {

  override def getUserFunction(parameters: CoGroupParameters[LeftIn, RightIn, Out]) = parameters.userFunction.right.get

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector[PactRecord]) = {

    val firstLeftRecord = leftIterator.initialize(leftRecords)
    outputRecord.copyFrom(firstLeftRecord, leftForward, leftForward)

    val firstRightRecord = rightIterator.initialize(rightRecords)
    outputRecord.copyFrom(firstRightRecord, rightForward, rightForward)

    val output = userFunction.apply(leftIterator, rightIterator)

    if (output.nonEmpty) {

      for (item <- output) {
        serializer.serialize(item, outputRecord)
        out.collect(outputRecord)
      }
    }
  }
}
