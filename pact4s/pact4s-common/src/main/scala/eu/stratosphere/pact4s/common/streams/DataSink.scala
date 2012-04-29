package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.Hintable
import eu.stratosphere.pact4s.common.analyzer.UDT

import eu.stratosphere.pact.common.io.FileOutputFormat
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.GenericDataSink

case class DataSink[T: UDT, S](url: String, formatter: T => S) extends Hintable {

}
