package eu.stratosphere.pact4s.common.streams

import java.io.OutputStream

import eu.stratosphere.pact4s.common.Hintable
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.io.FileOutputFormat
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.GenericDataSink

case class DataSink[In: UDT, F: UDF2Builder[In, OutputStream, Unit]#UDF](url: String, writeFunction: (In, OutputStream) => Unit) extends Hintable {

  val writeUDF = implicitly[UDF2[(In, OutputStream) => Unit]]
}

case class PlanOutput[In: UDT](source: DataStream[In], sink: DataSink[In, _]) {

  def getContract: Pact4sDataSinkContract = {

    val stub = classOf[File4sOutputStub[In]]
    val inputUDT = implicitly[UDT[In]]
    val writeUDF = sink.writeUDF
    val writeFunction = sink.writeFunction
    val name = sink.getPactName getOrElse "<Unnamed File Data Sink>"

    new FileDataSink(stub, sink.url, source.getContract, name) with FileDataSink4sContract[In] {

      sink.applyHints(this)

      override val stubParameters = OutputParameters(inputUDT, writeUDF, writeFunction)
    }
  }
}

object PlanOutput {

  implicit def planOutput2Seq[In](p: PlanOutput[In]): Seq[PlanOutput[In]] = Seq(p)
}