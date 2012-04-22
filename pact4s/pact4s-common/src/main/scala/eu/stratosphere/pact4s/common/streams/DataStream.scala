package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.Hintable

abstract class DataStream[T] extends Hintable

case class WrappedDataStream[T](inner: DataStream[T])
