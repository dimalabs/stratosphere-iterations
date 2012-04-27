package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait ReduceOperator[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def groupBy[Key, GroupByKeySelector: KeyBuilder[In, Key]#Selector](keySelector: In => Key) = new {

    def reduce[Out: UDT, F: UDF1Builder[Iterator[In], Out]#UDF](reduceFunction: Iterator[In] => Out) = {
      implicit val dummyCombinerUDF = new AnalyzedUDF1[Iterable[In], In](0, 0)
      new ReduceStream(input, keySelector, None, reduceFunction)
    }

    def combine[F: UDF1Builder[Iterator[In], In]#UDF](combineFunction: Iterator[In] => In) = new ReduceStream(input, keySelector, Some(combineFunction), combineFunction) {

      private val outer = this
      def reduce[Out: UDT, F: UDF1Builder[Iterator[In], Out]#UDF](reduceFunction: Iterator[In] => Out) = new ReduceStream(input, keySelector, combineFunction, reduceFunction) {
        override def getHints = if (this.hints == null) outer.hints else this.hints
      }
    }
  }
}
