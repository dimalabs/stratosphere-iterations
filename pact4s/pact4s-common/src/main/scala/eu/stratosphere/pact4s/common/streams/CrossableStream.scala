package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

import eu.stratosphere.pact4s.common.PactReadWriteSet
import eu.stratosphere.pact4s.common.PactSerializerFactory

trait CrossableStream[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cross[RightIn](rightInput: DataStream[RightIn]) = new {

    def map[Out](mapper: (LeftIn, RightIn) => Out)(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new CrossStream(leftInput, rightInput, mapper)

    def flatMap[Out](mapper: (LeftIn, RightIn) => GenTraversableOnce[Out])(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new FlatCrossStream(leftInput, rightInput, mapper)
  }
}

case class CrossStream[LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => Out)
  (implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]

case class FlatCrossStream[LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => GenTraversableOnce[Out])
  (implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]