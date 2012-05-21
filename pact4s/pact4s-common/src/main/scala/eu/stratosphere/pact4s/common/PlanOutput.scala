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