package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

trait MappableStream[In] { self: WrappedDataStream[In] =>

  val input = this.inner

  def map[Out](mapper: In => Out) = new MapStream(input, mapper)

  def flatMap[Out](mapper: In => GenTraversableOnce[Out]) = new FlatMapStream(input, mapper)

  def filter(predicate: In => Boolean) = new FlatMapStream(input, { x: In => if (predicate(x)) Some(x) else None })
}

case class MapStream[In, Out](
  input: DataStream[In],
  mapper: In => Out)
  extends DataStream[Out]

case class FlatMapStream[In, Out](
  input: DataStream[In],
  mapper: In => GenTraversableOnce[Out])
  extends DataStream[Out]
