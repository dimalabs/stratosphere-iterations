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

case class JoinParameters[LeftIn, RightIn, Out](
  val leftDeserializer: UDTSerializer[LeftIn],
  val leftDiscard: Array[Int],
  val rightDeserializer: UDTSerializer[RightIn],
  val rightForward: Array[Int],
  val serializer: UDTSerializer[Out],
  val outputLength: Int,
  val userFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]])
  extends StubParameters

class Join4sStub[LeftIn, RightIn, Out] extends MatchStub {

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var leftDiscard: Array[Int] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var rightForward: Array[Int] = _
  private var serializer: UDTSerializer[Out] = _
  private var outputLength: Int = _

  private var userFunction: (PactRecord, PactRecord, Collector[PactRecord]) => Unit = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[JoinParameters[LeftIn, RightIn, Out]](config)

    this.leftDeserializer = parameters.leftDeserializer
    this.leftDiscard = parameters.leftDiscard.filter(_ < parameters.outputLength)
    this.rightDeserializer = parameters.rightDeserializer
    this.rightForward = parameters.rightForward
    this.serializer = parameters.serializer
    this.outputLength = parameters.outputLength

    this.userFunction = parameters.userFunction.fold(doJoin _, doFlatJoin _)
  }

  override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = userFunction(leftRecord, rightRecord, out)

  private def doJoin(userFunction: (LeftIn, RightIn) => Out)(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) {

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

  private def doFlatJoin(userFunction: (LeftIn, RightIn) => Iterator[Out])(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) {

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
