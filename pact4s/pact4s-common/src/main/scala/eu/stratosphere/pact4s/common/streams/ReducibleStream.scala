package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.PactReadWriteSet
import eu.stratosphere.pact4s.common.PactSerializerFactory

trait ReducibleStream[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def groupBy[Key <% Comparable[Key]](keySelector: In => Key) = new {

    def reduce[Out](reducer: Iterable[In] => Out)(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new ReduceStream(input, keySelector, None, reducer)

    def combine(combiner: Iterable[In] => In)(implicit rwEv: PactReadWriteSet) = new CombineStream(input, keySelector, combiner, input.serializerFactory, rwEv)
  }
}

class CombineStream[Key, In](
  input: DataStream[In],
  keySelector: In => Key,
  combiner: Iterable[In] => In, 
  serEv: PactSerializerFactory[In],
  rwEv: PactReadWriteSet)
  (implicit keyEv: Key => Comparable[Key])
  extends ReduceStream[Key, In, In](input, keySelector, Some(combiner), combiner)(keyEv, serEv, rwEv) {

  private val outer = this
  def reduce[Out](reducer: Iterable[In] => Out)(implicit serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet) = new ReduceStream(input, keySelector, Some(combiner), reducer) {
    override def getHints = if (this.hints == null) outer.hints else this.hints
  }
}

case class ReduceStream[Key, In, Out](
  input: DataStream[In],
  keySelector: In => Key,
  combiner: Option[Iterable[In] => In],
  reducer: Iterable[In] => Out)
  (implicit keyEv: Key => Comparable[Key], serEv: PactSerializerFactory[Out], rwEv: PactReadWriteSet)
  extends DataStream[Out]
