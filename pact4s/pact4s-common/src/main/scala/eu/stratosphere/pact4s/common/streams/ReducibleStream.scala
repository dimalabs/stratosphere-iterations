package eu.stratosphere.pact4s.common.streams

trait ReducibleStream[In] { self: WrappedDataStream[In] =>

  val input = this.inner

  def groupBy[Key <% Comparable[Key]](keySelector: In => Key) = new {

    def reduce[Out](reducer: Iterable[In] => Out) = new ReduceStream(input, keySelector, None, reducer)

    def combine(combiner: Iterable[In] => In) = new AwaitingReducer(input, keySelector, combiner)
  }

  class AwaitingReducer[Key <% Comparable[Key]](input: DataStream[In], keySelector: In => Key, combiner: Iterable[In] => In) extends ReduceStream[Key, In, In](input, keySelector, Some(combiner), combiner) {

    private def outerHints() = this.getHints

    def reduce[Out](reducer: Iterable[In] => Out) = new ReduceStream(input, keySelector, Some(combiner), reducer) {
      override def getHints = if (this.hints == null) outerHints() else this.hints
    }
  }
}

case class ReduceStream[Key <% Comparable[Key], In, Out](
  input: DataStream[In],
  keySelector: In => Key,
  combiner: Option[Iterable[In] => In],
  reducer: Iterable[In] => Out)
  extends DataStream[Out]
