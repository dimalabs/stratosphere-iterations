package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.streams.DataStream
import eu.stratosphere.pact4s.common.streams.WrappedDataStream
import eu.stratosphere.pact4s.common.analyzer.UDT

package object operators {

  implicit def dataStream2CoGroup[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with CoGroupOperator[T]
  implicit def dataStream2Cross[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with CrossOperator[T]
  implicit def dataStream2Iterate[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with IterateOperator[T]
  implicit def dataStream2Join[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with JoinOperator[T]
  implicit def dataStream2Map[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with MapOperator[T]
  implicit def dataStream2Reduce[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with ReduceOperator[T]
}