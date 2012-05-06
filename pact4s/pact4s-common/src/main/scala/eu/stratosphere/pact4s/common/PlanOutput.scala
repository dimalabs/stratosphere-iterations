package eu.stratosphere.pact4s.common

import java.net.URI

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts.Pact4sDataSinkContract

import eu.stratosphere.pact.common.io.FileOutputFormat
import eu.stratosphere.pact.common.io.OutputFormat
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.GenericDataSink

case class PlanOutput[In: UDT](source: DataStream[In], sink: DataSink[In]) {

  private def createContract: Pact4sDataSinkContract[In] = new URI(sink.url).getScheme match {

    case "file" | null => new FileDataSink(sink.format.stub.asInstanceOf[Class[FileOutputFormat]], sink.url, source.getContract) with Pact4sDataSinkContract[In] {

      override val inputUDT = sink.format.inputUDT
      override val fieldSelector = sink.format.fieldSelector

      override def persistConfiguration() = sink.format.persistConfiguration(this.getParameters())
    }
  }

  lazy val getContract: Pact4sDataSinkContract[In] = {

    val contract = createContract
    sink.applyHints(contract)
    contract
  }
}

object PlanOutput {

  implicit def planOutput2Seq[In](p: PlanOutput[In]): Seq[PlanOutput[In]] = Seq(p)
}