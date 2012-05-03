package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait ReduceOperator[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def groupBy[Key, GroupByKeySelector: SelectorBuilder[In, Key]#Selector](keySelector: In => Key) = new {

    def reduce[Out: UDT, F: UDF1Builder[Iterator[In], Out]#UDF](reduceFunction: Iterator[In] => Out): DataStream[Out] = {

      implicit val dummyCombinerUDF = new AnalyzedUDF1[Iterable[In], In](0, 0)
      new ReduceStream(input, keySelector, None, reduceFunction)
    }

    def combine[F: UDF1Builder[Iterator[In], In]#UDF](combineFunction: Iterator[In] => In): DataStream[In] = new ReduceStream(input, keySelector, Some(combineFunction), combineFunction) {

      private val outer = this

      def reduce[Out: UDT, F: UDF1Builder[Iterator[In], Out]#UDF](reduceFunction: Iterator[In] => Out): DataStream[Out] = new ReduceStream(input, keySelector, Some(combineFunction), reduceFunction) {

        override def getHints = if (this.hints == null) outer.hints else this.hints
      }
    }
  }

  private class ReduceStream[Key, Out: UDT, GroupByKeySelector: SelectorBuilder[In, Key]#Selector, FC: UDF1Builder[Iterator[In], In]#UDF, FR: UDF1Builder[Iterator[In], Out]#UDF](
    input: DataStream[In],
    keySelector: In => Key,
    combineFunction: Option[Iterator[In] => In],
    reduceFunction: Iterator[In] => Out)
    extends DataStream[Out] {

    override def contract = {

      val stub = combineFunction map { _ => classOf[CombinableReduce4sStub[In, Out]] } getOrElse classOf[Reduce4sStub[In, Out]]
      val name = getPactName getOrElse "<Unnamed Reducer>"

      val keyFieldSelector = implicitly[FieldSelector[In => Key]]
      val keyFieldTypes = implicitly[UDT[In]].getKeySet(keyFieldSelector.getFields)

      new ReduceContract(stub, keyFieldTypes, keyFieldSelector.getFields, input.getContract, name) with Reduce4sContract[Key, In, Out] {

        override val keySelector = keyFieldSelector
        override val inputUDT = implicitly[UDT[In]]
        override val outputUDT = implicitly[UDT[Out]]
        override val combineUDF = implicitly[UDF1[Iterator[In] => In]]
        override val reduceUDF = implicitly[UDF1[Iterator[In] => Out]]
        override val userCombineFunction = combineFunction
        override val userReduceFunction = reduceFunction
      }
    }
  }
}