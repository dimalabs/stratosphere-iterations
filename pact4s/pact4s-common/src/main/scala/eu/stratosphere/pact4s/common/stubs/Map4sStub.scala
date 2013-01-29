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

case class MapParameters[In, Out](
  val deserializer: UDTSerializer[In],
  val serializer: UDTSerializer[Out],
  val discard: Array[Int],
  val outputLength: Int,
  val userFunction: MapParameters.FunType[In, Out])
  extends StubParameters

object MapParameters {
  
  type FunType[In, Out] = Either[In => Out, In => Iterator[Out]]

  def getStubFor[In, Out](mapFunction: FunType[In, Out]) = mapFunction match {
    case Left(_)  => classOf[Map4sStub[In, Out]]
    case Right(_) => classOf[FlatMap4sStub[In, Out]]
  }
}

abstract sealed class Map4sStubBase[In, Out, R] extends MapStub {

  protected var deserializer: UDTSerializer[In] = _
  protected var serializer: UDTSerializer[Out] = _
  protected var discard: Array[Int] = _
  protected var outputLength: Int = _
  protected var userFunction: In => R = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[MapParameters[In, Out]](config)

    this.deserializer = parameters.deserializer
    this.serializer = parameters.serializer
    this.discard = parameters.discard
    this.outputLength = parameters.outputLength
    this.userFunction = getUserFunction(parameters)
  }

  protected def getUserFunction(parameters: MapParameters[In, Out]): In => R
}

final class Map4sStub[In, Out] extends Map4sStubBase[In, Out, Out] {

  override def getUserFunction(parameters: MapParameters[In, Out]) = parameters.userFunction.left.get

  override def map(record: PactRecord, out: Collector[PactRecord]) = {

    val input = deserializer.deserializeRecyclingOn(record)
    val output = userFunction.apply(input)

    record.setNumFields(outputLength)

    for (field <- discard)
      record.setNull(field)

    serializer.serialize(output, record)
    out.collect(record)
  }
}

final class FlatMap4sStub[In, Out] extends Map4sStubBase[In, Out, Iterator[Out]] {

  override def getUserFunction(parameters: MapParameters[In, Out]) = parameters.userFunction.right.get

  override def map(record: PactRecord, out: Collector[PactRecord]) = {

    val input = deserializer.deserializeRecyclingOn(record)
    val output = userFunction.apply(input)

    if (output.nonEmpty) {

      record.setNumFields(outputLength)

      for (field <- discard)
        record.setNull(field)

      for (item <- output) {

        serializer.serialize(item, record)
        out.collect(record)
      }
    }
  }
}
