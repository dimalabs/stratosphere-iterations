package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

import eu.stratosphere.pact4s.common.analyzer._

trait CrossableStream[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cross[RightIn](rightInput: DataStream[RightIn]) = new {

    def map[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](mapper: (LeftIn, RightIn) => Out) = new CrossStream(leftInput, rightInput, mapper)

    def flatMap[Out: UDT, F: UDF2Builder[LeftIn, RightIn, GenTraversableOnce[Out]]#UDF](mapper: (LeftIn, RightIn) => GenTraversableOnce[Out]) = new FlatCrossStream(leftInput, rightInput, mapper)
  }
}

case class CrossStream[LeftIn: UDT, RightIn: UDT, Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => Out)
  extends DataStream[Out] {
  
  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}

case class FlatCrossStream[LeftIn: UDT, RightIn: UDT, Out: UDT, F: UDF2Builder[LeftIn, RightIn, GenTraversableOnce[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => GenTraversableOnce[Out])
  extends DataStream[Out] {
  
  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}