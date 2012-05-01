package eu.stratosphere.pact4s.common

import java.net.URI

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts.Pact4sDataSinkContract

import eu.stratosphere.pact.common.io.FileOutputFormat
import eu.stratosphere.pact.common.io.OutputFormat
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.GenericDataSink

case class PlanOutput[In: UDT](source: DataStream[In], sink: DataSink[In]) {

  def getContract: Pact4sDataSinkContract = {

    val name = sink.getPactName getOrElse "<Unnamed File Data Sink>"
    val uri = new URI(sink.url)

    val contract = uri.getScheme match {
      case "file" | null => new FileDataSink(sink.format.stub.asInstanceOf[Class[FileOutputFormat]], sink.url, source.getContract, name) with Pact4sDataSinkContract {
        override def persistConfiguration() = sink.format.configure(this.getParameters())
      }
    }

    sink.applyHints(contract)
    contract
  }
}

object PlanOutput {

  implicit def planOutput2Seq[In, S](p: PlanOutput[In]): Seq[PlanOutput[In]] = Seq(p)
}