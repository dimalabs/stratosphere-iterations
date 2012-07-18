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

package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class MapParameters[In, Out](
  val deserializer: UDTSerializer[In],
  val serializer: UDTSerializer[Out],
  val discard: Array[Int],
  val userFunction: Either[In => Out, In => Iterator[Out]])
  extends StubParameters

class Map4sStub[In, Out] extends MapStub {

  private var deserializer: UDTSerializer[In] = _
  private var serializer: UDTSerializer[Out] = _
  private var discard: Array[Int] = _

  private var userFunction: (PactRecord, Collector[PactRecord]) => Unit = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[MapParameters[In, Out]](config)

    this.deserializer = parameters.deserializer
    this.serializer = parameters.serializer
    this.discard = parameters.discard

    this.userFunction = parameters.userFunction.fold(doMap _, doFlatMap _)
  }

  override def map(record: PactRecord, out: Collector[PactRecord]) = userFunction(record, out)

  private def doMap(userFunction: In => Out)(record: PactRecord, out: Collector[PactRecord]) = {

    val input = deserializer.deserialize(record)
    val output = userFunction.apply(input)

    for (field <- discard)
      record.setNull(field)

    serializer.serialize(output, record)
    out.collect(record)
  }

  private def doFlatMap(userFunction: In => Iterator[Out])(record: PactRecord, out: Collector[PactRecord]) = {

    val input = deserializer.deserialize(record)
    val output = userFunction.apply(input)

    if (output.nonEmpty) {

      for (field <- discard)
        record.setNull(field)

      for (item <- output) {

        serializer.serialize(item, record)
        out.collect(record)
      }
    }
  }
}
