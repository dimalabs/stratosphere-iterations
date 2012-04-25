package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.Hintable

case class DataSink[T, S](url: String, formatter: T => S) extends Hintable
