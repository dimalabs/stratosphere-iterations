package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

trait JoinableStream[LeftIn] { self: WrappedDataStream[LeftIn] =>

  val leftInput = this.inner

  def join[RightIn](rightInput: DataStream[RightIn]) = new {

    def on[Key <% Comparable[Key]](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo(rightKeySelector: RightIn => Key) = new {

        def map[Out](mapper: (LeftIn, RightIn) => Out) = new JoinStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)

        def flatMap[Out](mapper: (LeftIn, RightIn) => GenTraversableOnce[Out]) = new FlatJoinStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)
      }
    }
  }
}

case class JoinStream[Key <% Comparable[Key], LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (LeftIn, RightIn) => Out)
  extends DataStream[Out]

case class FlatJoinStream[Key <% Comparable[Key], LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (LeftIn, RightIn) => GenTraversableOnce[Out])
  extends DataStream[Out]