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

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.nephele.configuration.Configuration

case class CopyParameters(
  val from: Array[Int],
  val to: Array[Int],
  val discard: Array[Int])
  extends StubParameters

class Copy4sStub extends MapStub {

  private var from: Array[Int] = _
  private var to: Array[Int] = _
  private var discard: Array[Int] = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[CopyParameters](config)

    this.from = parameters.from
    this.to = parameters.to
    this.discard = parameters.discard
  }

  override def map(record: PactRecord, out: Collector[PactRecord]) = {

    record.copyFrom(record, from, to)

    for (field <- discard)
      record.setNull(field)

    out.collect(record)
  }
}