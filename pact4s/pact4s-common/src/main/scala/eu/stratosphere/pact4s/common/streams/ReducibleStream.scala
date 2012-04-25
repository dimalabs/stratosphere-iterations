package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._

trait ReducibleStream[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def groupBy[Key, GroupByKeySelector: KeyBuilder[In, Key]#Selector](keySelector: In => Key) = new {

    def reduce[Out: UDT, F: UDF1Builder[Iterable[In], Out]#UDF](reducer: Iterable[In] => Out) = new ReduceStream(input, keySelector, None, reducer)

    def combine[F: UDF1Builder[Iterable[In], In]#UDF](combiner: Iterable[In] => In) = new CombineStream(input, keySelector, combiner)
  }
}

case class ReduceStream[Key, In: UDT, Out: UDT, GroupByKeySelector: KeyBuilder[In, Key]#Selector, FC: UDF1Builder[Iterable[In], In]#UDF, FR: UDF1Builder[Iterable[In], Out]#UDF](
  input: DataStream[In],
  keySelector: In => Key,
  combiner: Option[Iterable[In] => In],
  reducer: Iterable[In] => Out)
  extends DataStream[Out] {
  
  val combinerUDF = implicitly[UDF1[Iterable[In] => In]]
  val reducerUDF = implicitly[UDF1[Iterable[In] => Out]]
  
  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}

class CombineStream[Key, In: UDT, GroupByKeySelector: KeyBuilder[In, Key]#Selector, F: UDF1Builder[Iterable[In], In]#UDF](
  input: DataStream[In],
  keySelector: In => Key,
  combiner: Iterable[In] => In)
  extends ReduceStream[Key, In, In, GroupByKeySelector, F, F](input, keySelector, Some(combiner), combiner) {

  private val outer = this
  def reduce[Out: UDT, F: UDF1Builder[In, Out]#UDF](reducer: Iterable[In] => Out) = new ReduceStream(input, keySelector, Some(combiner), reducer) {
    override def getHints = if (this.hints == null) outer.hints else this.hints
  }

  override def getContract = throw new UnsupportedOperationException("Not implemented yet")
}

