package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

trait CrossableStream[LeftIn] { self: WrappedDataStream[LeftIn] =>

  val leftInput = this.inner

  def cross[RightIn](rightInput: DataStream[RightIn]) = new {

    def map[Out](mapper: (LeftIn, RightIn) => Out) = new CrossStream(leftInput, rightInput, mapper)

    def flatMap[Out](mapper: (LeftIn, RightIn) => GenTraversableOnce[Out]) = new FlatCrossStream(leftInput, rightInput, mapper)
  }
}

case class CrossStream[LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => Out)
  extends DataStream[Out]

case class FlatCrossStream[LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => GenTraversableOnce[Out])
  extends DataStream[Out]