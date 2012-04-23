package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

import eu.stratosphere.pact4s.common.PactReadWriteSet
import eu.stratosphere.pact4s.common.PactSerializerFactory

trait MappableStream[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def map[Out](mapper: In => Out)(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new MapStream(input, mapper)

  def flatMap[Out](mapper: In => GenTraversableOnce[Out])(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new FlatMapStream(input, mapper)

  def filter(predicate: In => Boolean)(implicit serEv: PactSerializerFactory[In], rwEv: PactReadWriteSet) = new FlatMapStream(input, { x: In => if (predicate(x)) Some(x) else None })
}

case class MapStream[In, Out](
  input: DataStream[In],
  mapper: In => Out)
  (implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]

case class FlatMapStream[In, Out](
  input: DataStream[In],
  mapper: In => GenTraversableOnce[Out])
  (implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]
