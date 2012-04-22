package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

trait CoGroupableStream[LeftIn] { self: WrappedDataStream[LeftIn] =>

  val leftInput = this.inner

  def cogroup[RightIn](rightInput: DataStream[RightIn]) = new {

    def on[Key <% Comparable[Key]](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo(rightKeySelector: RightIn => Key) = new {

        def map[Out](mapper: (Iterable[LeftIn], Iterable[RightIn]) => Out) = new CoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)

        def flatMap[Out](mapper: (Iterable[LeftIn], Iterable[RightIn]) => GenTraversableOnce[Out]) = new FlatCoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)
      }
    }
  }
}

case class CoGroupStream[Key <% Comparable[Key], LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (Iterable[LeftIn], Iterable[RightIn]) => Out)
  extends DataStream[Out]

case class FlatCoGroupStream[Key <% Comparable[Key], LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (Iterable[LeftIn], Iterable[RightIn]) => GenTraversableOnce[Out])
  extends DataStream[Out]