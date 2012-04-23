package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

import eu.stratosphere.pact4s.common.PactReadWriteSet
import eu.stratosphere.pact4s.common.PactSerializerFactory

trait CoGroupableStream[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cogroup[RightIn](rightInput: DataStream[RightIn]) = new {

    def on[Key <% Comparable[Key]](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo(rightKeySelector: RightIn => Key) = new {

        def map[Out](mapper: (Iterable[LeftIn], Iterable[RightIn]) => Out)(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new CoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)

        def flatMap[Out](mapper: (Iterable[LeftIn], Iterable[RightIn]) => GenTraversableOnce[Out])(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new FlatCoGroupStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)
      }
    }
  }
}

case class CoGroupStream[Key, LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (Iterable[LeftIn], Iterable[RightIn]) => Out)
  (implicit keyEv: Key => Comparable[Key], serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]

case class FlatCoGroupStream[Key, LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (Iterable[LeftIn], Iterable[RightIn]) => GenTraversableOnce[Out])
  (implicit keyEv: Key => Comparable[Key], serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]