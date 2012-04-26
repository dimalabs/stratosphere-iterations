package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.util.ForEachAble

case class CrossStream[LeftIn: UDT, RightIn: UDT, Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => Out)
  extends DataStream[Out] {
  
  override def contract = throw new UnsupportedOperationException("Not implemented yet")
}

case class FlatCrossStream[LeftIn: UDT, RightIn: UDT, Out: UDT, F: UDF2Builder[LeftIn, RightIn, ForEachAble[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapper: (LeftIn, RightIn) => ForEachAble[Out])
  extends DataStream[Out] {
  
  override def contract = throw new UnsupportedOperationException("Not implemented yet")
}