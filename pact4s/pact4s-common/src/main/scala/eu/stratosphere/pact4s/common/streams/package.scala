package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analyzer._

package object streams {

  type ForEachAble[A] = { def foreach[U](f: A => U): Unit }
  
  implicit def dataStream2CoGroupableStream[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with CoGroupableStream[T]
  implicit def dataStream2CrossableStream[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with CrossableStream[T]
  implicit def dataStream2JoinableStream[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with JoinableStream[T]
  implicit def dataStream2MappableStream[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with MappableStream[T]
  implicit def dataStream2RecursibleStream[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with RecursibleStream[T]
  implicit def dataStream2ReducibleStream[T: UDT](input: DataStream[T]) = new WrappedDataStream(input) with ReducibleStream[T]
}