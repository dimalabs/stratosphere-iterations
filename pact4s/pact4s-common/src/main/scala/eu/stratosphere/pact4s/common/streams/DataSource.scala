package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._

case class DataSource[S, T: UDT, F: UDF1Builder[S, T]#UDF](url: String, parser: S => T) extends DataStream[T] {

  val udf = implicitly[UDF1[S => T]]
  override def contract = throw new UnsupportedOperationException("Not implemented yet")
}
