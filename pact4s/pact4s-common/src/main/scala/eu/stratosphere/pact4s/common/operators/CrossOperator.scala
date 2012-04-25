package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait CrossOperator[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cross[RightIn](rightInput: DataStream[RightIn]) = new {

    def map[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](mapper: (LeftIn, RightIn) => Out) = new CrossStream(leftInput, rightInput, mapper)

    def flatMap[Out: UDT, F: UDF2Builder[LeftIn, RightIn, ForEachAble[Out]]#UDF](mapper: (LeftIn, RightIn) => ForEachAble[Out]) = new FlatCrossStream(leftInput, rightInput, mapper)
  }
}

