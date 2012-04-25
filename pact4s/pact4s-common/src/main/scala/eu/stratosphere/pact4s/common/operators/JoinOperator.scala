package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.util.ForEachAble

trait JoinOperator[LeftIn] { this: WrappedDataStream[LeftIn] =>

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
