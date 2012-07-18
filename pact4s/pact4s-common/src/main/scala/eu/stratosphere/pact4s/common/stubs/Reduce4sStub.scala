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

import java.util.{ Iterator => JIterator }

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable
import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class ReduceParameters[In, Out](
  val combineDeserializer: Option[UDTSerializer[In]],
  val combineSerializer: Option[UDTSerializer[In]],
  val combineForward: Option[Array[Int]],
  val combineFunction: Option[Iterator[In] => In],
  val reduceDeserializer: UDTSerializer[In],
  val reduceSerializer: UDTSerializer[Out],
  val reduceForward: Array[Int],
  val reduceFunction: Iterator[In] => Out)
  extends StubParameters

class Reduce4sStub[In, Out] extends ReduceStub {

  private val combineRecord = new PactRecord()
  private val reduceRecord = new PactRecord()

  private var combineIterator: DeserializingIterator[In] = null
  private var combineForward: Array[Int] = _
  private var combineFunction: Iterator[In] => In = _
  private var combineSerializer: UDTSerializer[In] = _

  private var reduceIterator: DeserializingIterator[In] = null
  private var reduceForward: Array[Int] = _
  private var reduceFunction: Iterator[In] => Out = _
  private var reduceSerializer: UDTSerializer[Out] = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[ReduceParameters[In, Out]](config)

    this.combineIterator = parameters.combineDeserializer map { d => new DeserializingIterator(d) } getOrElse null
    this.combineForward = parameters.combineForward getOrElse null
    this.combineFunction = parameters.combineFunction getOrElse null
    this.combineSerializer = parameters.combineSerializer getOrElse null

    this.reduceIterator = new DeserializingIterator(parameters.reduceDeserializer)
    this.reduceForward = parameters.reduceForward
    this.reduceFunction = parameters.reduceFunction
    this.reduceSerializer = parameters.reduceSerializer
  }

  override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

    combineIterator.initialize(records)
    combineRecord.copyFrom(combineIterator.getFirstRecord, combineForward, combineForward);

    val output = combineFunction.apply(combineIterator)

    combineSerializer.serialize(output, combineRecord)
    out.collect(combineRecord)
  }

  override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

    reduceIterator.initialize(records)
    reduceRecord.copyFrom(reduceIterator.getFirstRecord, reduceForward, reduceForward);

    val output = reduceFunction.apply(reduceIterator)

    reduceSerializer.serialize(output, reduceRecord)
    out.collect(reduceRecord)
  }
}
