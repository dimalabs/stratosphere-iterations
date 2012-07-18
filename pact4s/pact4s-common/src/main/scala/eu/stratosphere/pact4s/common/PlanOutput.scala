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

package eu.stratosphere.pact4s.common

import java.net.URI

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts.Pact4sContractFactory
import eu.stratosphere.pact4s.common.contracts.DataSink4sContract

import eu.stratosphere.pact.common.io.FileOutputFormat
import eu.stratosphere.pact.common.generic.io.OutputFormat
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.GenericDataSink

class PlanOutput[In: UDT](val source: DataStream[In], val sink: DataSink[In]) extends Pact4sContractFactory with Serializable {

  override def getHints = sink.getHints

  override def createContract = {

    val uri = getUri(sink.url)
    uri.getScheme match {

      case "file" | "hdfs" => new FileDataSink(sink.format.stub.asInstanceOf[Class[FileOutputFormat]], uri.toString, source.getContract) with DataSink4sContract[In] {

        override val inputUDT = sink.format.inputUDT
        override val fieldSelector = sink.format.fieldSelector

        override def persistConfiguration() = sink.format.persistConfiguration(this.getParameters())
      }
    }
  }

  private def getUri(url: String) = {
    val uri = new URI(url)
    if (uri.getScheme == null)
      new URI("file://" + url)
    else
      uri
  }
}

object PlanOutput {

  implicit def planOutput2Seq[In](p: PlanOutput[In]): Seq[PlanOutput[In]] = Seq(p)
}