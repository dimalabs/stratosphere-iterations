package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait CoGroupOperator[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cogroup[RightIn: UDT](rightInput: DataStream[RightIn]) = new {

    def on[Key, LeftKeySelector: SelectorBuilder[LeftIn, Key]#Selector](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo[Key, RightKeySelector: SelectorBuilder[RightIn, Key]#Selector](rightKeySelector: RightIn => Key) = new {

        def map[Out: UDT, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Out]#UDF](mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out) = new CoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapFunction)

        def flatMap[Out: UDT, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Iterator[Out]]#UDF](mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]) = new FlatCoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapFunction)
      }
    }
  }
}
