package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait CoGroupOperator[LeftIn] { this: WrappedDataStream[LeftIn] =>

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
