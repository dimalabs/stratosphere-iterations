package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait CrossOperator[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cross[RightIn: UDT](rightInput: DataStream[RightIn]) = new {

    def map[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](mapFunction: (LeftIn, RightIn) => Out) = new CrossStream(leftInput, rightInput, mapFunction)

    def flatMap[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Iterator[Out]]#UDF](mapFunction: (LeftIn, RightIn) => Iterator[Out]) = new FlatCrossStream(leftInput, rightInput, mapFunction)
  }
}

