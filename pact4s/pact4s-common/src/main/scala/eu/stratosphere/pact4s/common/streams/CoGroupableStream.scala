package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._

trait CoGroupableStream[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cogroup[RightIn: UDT](rightInput: DataStream[RightIn]) = new {

    def on[Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo[Key, RightKeySelector: KeyBuilder[RightIn, Key]#Selector](rightKeySelector: RightIn => Key) = new {

        def map[Out: UDT, F: UDF2Builder[Iterable[LeftIn], Iterable[RightIn], Out]#UDF](mapper: (Iterable[LeftIn], Iterable[RightIn]) => Out) = new CoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)

        def flatMap[Out: UDT, F: UDF2Builder[Iterable[LeftIn], Iterable[RightIn], ForEachAble[Out]]#UDF](mapper: (Iterable[LeftIn], Iterable[RightIn]) => ForEachAble[Out]) = new FlatCoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)
      }
    }
  }
}

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