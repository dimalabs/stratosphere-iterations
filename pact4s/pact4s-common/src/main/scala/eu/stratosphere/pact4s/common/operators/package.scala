package eu.stratosphere.pact4s.common

import scala.collection.TraversableOnce

import eu.stratosphere.pact4s.common.analyzer.UDT

package object operators {

  case class WrappedDataSink[T](inner: DataSink[T])
  case class WrappedDataStream[T](inner: DataStream[T])

  implicit def dataSink2SinkToSource[T: UDT](sink: DataSink[T]) = new WrappedDataSink(sink) with SinkToSourceOperator[T]
  implicit def dataStream2SourceToSink[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with SourceToSinkOperator[T]

  implicit def dataStream2CoGroup[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with CoGroupOperator[T]
  implicit def dataStream2Cross[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with CrossOperator[T]
  implicit def dataStream2Iterate[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with IterateOperator[T]
  implicit def dataStream2Join[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with JoinOperator[T]
  implicit def dataStream2Map[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with MapOperator[T]
  implicit def dataStream2Reduce[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with ReduceOperator[T]

  implicit def traversableToIterator[T](i: TraversableOnce[T]): Iterator[T] = i.toIterator
  implicit def optionToIterator[T](opt: Option[T]): Iterator[T] = opt.iterator
  implicit def arrayToIterator[T](arr: Array[T]): Iterator[T] = arr.iterator
}