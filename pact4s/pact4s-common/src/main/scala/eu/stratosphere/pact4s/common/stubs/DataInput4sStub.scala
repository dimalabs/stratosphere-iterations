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

import java.io.DataInput

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.nephele.template.GenericInputSplit;

case class BinaryInputParameters[Out](
  val serializer: UDTSerializer[Out],
  val userFunction: DataInput => Out)
  extends StubParameters

case class DelimetedInputParameters[Out](
  val serializer: UDTSerializer[Out],
  val userFunction: Array[Byte] => Out)
  extends StubParameters

case class FixedLengthInputParameters[Out](
  val serializer: UDTSerializer[Out],
  val userFunction: (Array[Byte], Int) => Out)
  extends StubParameters

case class ExternalProcessFixedLengthInputParameters[Out](
  val serializer: UDTSerializer[Out],
  val externalProcessCommand: Int => String,
  val numSplits: Option[Int],
  val userFunction: (Array[Byte], Int) => Out)
  extends StubParameters

class BinaryInput4sStub[Out] extends BinaryInputFormat {

  private var serializer: UDTSerializer[Out] = _
  private var userFunction: DataInput => Out = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[BinaryInputParameters[Out]](config)

    this.serializer = parameters.serializer
    this.userFunction = parameters.userFunction
  }

  override def deserialize(record: PactRecord, source: DataInput) = {

    val output = userFunction.apply(source)
    serializer.serialize(output, record)
  }
}

class DelimetedInput4sStub[Out] extends DelimitedInputFormat {

  private var serializer: UDTSerializer[Out] = _
  private var userFunction: Array[Byte] => Out = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[DelimetedInputParameters[Out]](config)

    this.serializer = parameters.serializer
    this.userFunction = parameters.userFunction
  }

  override def readRecord(record: PactRecord, source: Array[Byte], numBytes: Int): Boolean = {

    val output = userFunction.apply(source)

    if (output != null)
      serializer.serialize(output, record)

    return output != null
  }
}

class FixedLengthInput4sStub[Out] extends FixedLengthInputFormat {

  private var serializer: UDTSerializer[Out] = _
  private var userFunction: (Array[Byte], Int) => Out = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[FixedLengthInputParameters[Out]](config)

    this.serializer = parameters.serializer
    this.userFunction = parameters.userFunction
  }

  override def readBytes(record: PactRecord, source: Array[Byte], startPos: Int): Boolean = {

    val output = userFunction.apply(source, startPos)

    if (output != null)
      serializer.serialize(output, record)

    return output != null
  }
}

class ExternalProcessFixedLengthInput4sStub[Out] extends ExternalProcessFixedLengthInputFormat[ExternalProcessInputSplit] {

  private var serializer: UDTSerializer[Out] = _
  private var externalProcessCommand: Int => String = _
  private var numSplits: Int = _
  private var userFunction: (Array[Byte], Int) => Out = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[ExternalProcessFixedLengthInputParameters[Out]](config)

    this.serializer = parameters.serializer
    this.externalProcessCommand = parameters.externalProcessCommand
    this.numSplits = math.max(parameters.numSplits.getOrElse(1), 1)
    this.userFunction = parameters.userFunction
  }

  override def createInputSplits(minNumSplits: Int): Array[GenericInputSplit] = {
    (0 until math.max(minNumSplits, numSplits)) map { split => new ExternalProcessInputSplit(split, this.externalProcessCommand(split)) } toArray
  }

  override def readBytes(record: PactRecord, source: Array[Byte], startPos: Int): Boolean = {

    val output = userFunction.apply(source, startPos)

    if (output != null)
      serializer.serialize(output, record)

    return output != null
  }
}
