package eu.stratosphere.pact4s.common

package object streams {

  implicit def dataStream2CoGroupableStream[T: PactSerializerFactory](input: DataStream[T]) = new WrappedDataStream(input) with CoGroupableStream[T]
  implicit def dataStream2CrossableStream[T: PactSerializerFactory](input: DataStream[T]) = new WrappedDataStream(input) with CrossableStream[T]
  implicit def dataStream2JoinableStream[T: PactSerializerFactory](input: DataStream[T]) = new WrappedDataStream(input) with JoinableStream[T]
  implicit def dataStream2MappableStream[T: PactSerializerFactory](input: DataStream[T]) = new WrappedDataStream(input) with MappableStream[T]
  implicit def dataStream2RecursibleStream[T: PactSerializerFactory](input: DataStream[T]) = new WrappedDataStream(input) with RecursibleStream[T]
  implicit def dataStream2ReducibleStream[T: PactSerializerFactory](input: DataStream[T]) = new WrappedDataStream(input) with ReducibleStream[T]
}