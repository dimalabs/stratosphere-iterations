package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait ReduceOperator[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def groupBy[Key, GroupByKeySelector: KeyBuilder[In, Key]#Selector](keySelector: In => Key) = new {

    def reduce[Out: UDT, F: UDF1Builder[Iterable[In], Out]#UDF](reducer: Iterable[In] => Out) = {
      implicit val emptyCombinerUDF = new AnalyzedUDF1[Iterable[In], In](0, 0)
      new ReduceStream(input, keySelector, None, reducer)
    }

    def combine[F: UDF1Builder[Iterable[In], In]#UDF](combiner: Iterable[In] => In) = new CombineStream(input, keySelector, combiner)
  }
}
