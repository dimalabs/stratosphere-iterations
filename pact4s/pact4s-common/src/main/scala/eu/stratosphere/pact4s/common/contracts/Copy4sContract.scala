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

package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Copy4sContract[In] extends Pact4sOneInputContract[In, In] { this: MapContract =>

  override def annotations = Seq(
    Annotations.getConstantFields(udf.getForwardIndexArray),
    Annotations.getOutCardBounds(Annotations.CARD_INPUTCARD, Annotations.CARD_INPUTCARD))

  override def persistConfiguration() = {

    val stubParameters = CopyParameters(
      udf.inputFields.toSerializerIndexArray,
      udf.outputFields.toSerializerIndexArray,
      udf.getDiscardIndexArray.filter(_ < udf.getOutputLength),
      udf.getOutputLength)
    stubParameters.persist(this)
  }
}

object Copy4sContract {

  def newBuilder = MapContract.builder(CopyParameters.getStub)

  def apply[In](source: Pact4sContract[In]): Copy4sContract[In] = {
    new MapContract(Copy4sContract.newBuilder.input(source)) with Copy4sContract[In] {

      override val udf = new UDF1[In, In]()(source.getUDF.outputUDT, source.getUDF.outputUDT)

      override def persistHints() = {
        this.setName("Copy " + source.getName())
        this.getCompilerHints().setAvgBytesPerRecord(source.getCompilerHints().getAvgBytesPerRecord())
      }
    }
  }

  def unapply(c: Copy4sContract[_]) = Some(c.singleInput)
}
