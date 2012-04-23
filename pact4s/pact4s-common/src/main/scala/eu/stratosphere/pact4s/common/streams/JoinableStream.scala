package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

import eu.stratosphere.pact4s.common.PactReadWriteSet
import eu.stratosphere.pact4s.common.PactSerializerFactory

trait JoinableStream[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def join[RightIn](rightInput: DataStream[RightIn]) = new {

    def on[Key <% Comparable[Key]](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo(rightKeySelector: RightIn => Key) = new {

        def map[Out](mapper: (LeftIn, RightIn) => Out)(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new JoinStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)

        def flatMap[Out](mapper: (LeftIn, RightIn) => GenTraversableOnce[Out])(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new FlatJoinStream(leftInput, rightInput, leftKeySelector, rightKeySelector, mapper)
      }
    }
  }
}

case class JoinStream[Key, LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (LeftIn, RightIn) => Out)
  (implicit keyEv: Key => Comparable[Key], serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]

case class FlatJoinStream[Key, LeftIn, RightIn, Out](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapper: (LeftIn, RightIn) => GenTraversableOnce[Out])
  (implicit keyEv: Key => Comparable[Key], serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]