package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.util.ForEachAble

case class CoGroupStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterable[LeftIn], Iterable[RightIn], Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (Iterable[LeftIn], Iterable[RightIn]) => Out)
  extends DataStream[Out] {

  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}

case class FlatCoGroupStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterable[LeftIn], Iterable[RightIn], ForEachAble[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (Iterable[LeftIn], Iterable[RightIn]) => ForEachAble[Out])
  extends DataStream[Out] {

  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}