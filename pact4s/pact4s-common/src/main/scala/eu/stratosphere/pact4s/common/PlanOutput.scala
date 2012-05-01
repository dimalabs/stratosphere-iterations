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

    val DataSink(url, format) = sink
    val stub = format.stub
    val name = sink.getPactName getOrElse "<Unnamed File Data Sink>"

    val contract = new URI(sink.url).getScheme match {

      case "file" | null => new FileDataSink(stub.asInstanceOf[Class[FileOutputFormat]], url, source.getContract, name) with Pact4sDataSinkContract {

        override val inputUDT = format.inputUDT
        override val fieldSelector = format.fieldSelector

        override def persistConfiguration() = format.persistConfiguration(this.getParameters())
      }
    }

    sink.applyHints(contract)
    contract
  }
}

object PlanOutput {

  implicit def planOutput2Seq[In](p: PlanOutput[In]): Seq[PlanOutput[In]] = Seq(p)
}