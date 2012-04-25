package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.streams.DataSink
import eu.stratosphere.pact4s.common.streams.DataStream

case class PlanOutput(source: DataStream[_], sink: DataSink[_, _])

object PlanOutput {

  implicit def planOutput2Seq(p: PlanOutput): Seq[PlanOutput] = Seq(p)
}