package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._

trait JoinableStream[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def join[RightIn: UDT](rightInput: DataStream[RightIn]) = new {

    def on[Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo[RightKeySelector: KeyBuilder[RightIn, Key]#Selector](rightKeySelector: RightIn => Key) = new {

        def map[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](mapper: (LeftIn, RightIn) => Out) = new JoinStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)

        def flatMap[Out: UDT, F: UDF2Builder[LeftIn, RightIn, ForEachAble[Out]]#UDF](mapper: (LeftIn, RightIn) => ForEachAble[Out]) = new FlatJoinStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)
      }
    }
  }
}

case class JoinStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (LeftIn, RightIn) => Out)
  extends DataStream[Out] {
  
  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}

case class FlatJoinStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[LeftIn, RightIn, ForEachAble[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (LeftIn, RightIn) => ForEachAble[Out])
  extends DataStream[Out] {
  
  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}