package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.Hintable
import eu.stratosphere.pact4s.common.PactReadWriteSet
import eu.stratosphere.pact4s.common.PactSerializerFactory

abstract class DataStream[T](implicit serEv: PactSerializerFactory[T], rwEv: PactReadWriteSet) extends Hintable {
  val serializerFactory = implicitly[PactSerializerFactory[T]]
  val readWriteSet = implicitly[PactReadWriteSet]
}

case class WrappedDataStream[T](inner: DataStream[T])
