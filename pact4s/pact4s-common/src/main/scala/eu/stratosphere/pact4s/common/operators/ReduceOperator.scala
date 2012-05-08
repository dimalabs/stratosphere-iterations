package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class ReduceOperator[In: UDT](input: DataStream[In]) extends Serializable {

  def groupBy[Key, GroupByKeySelector: SelectorBuilder[In, Key]#Selector](keySelector: In => Key) = new Serializable {

    def reduce[Out: UDT, F: UDF1Builder[Iterator[In], Out]#UDF](reduceFunction: Iterator[In] => Out): DataStream[Out] = {

      implicit val dummyCombinerUDF = new AnalyzedUDF1[Iterator[In], In](0, 0)
      new ReduceStream(input, keySelector, None, reduceFunction)
    }

    def combine[F: UDF1Builder[Iterator[In], In]#UDF](combineFunction: Iterator[In] => In): DataStream[In] = new ReduceStream(input, keySelector, Some(combineFunction), combineFunction) {

      private val outer = this

      def reduce[Out: UDT, F: UDF1Builder[Iterator[In], Out]#UDF](reduceFunction: Iterator[In] => Out): DataStream[Out] = new ReduceStream(input, keySelector, Some(combineFunction), reduceFunction) {

        override def getHints = outer.getGenericHints ++ this.hints
      }
    }
  }

  private class ReduceStream[Key, Out: UDT, GroupByKeySelector: SelectorBuilder[In, Key]#Selector, FC: UDF1Builder[Iterator[In], In]#UDF, FR: UDF1Builder[Iterator[In], Out]#UDF](
    input: DataStream[In],
    keySelector: In => Key,
    combineFunction: Option[Iterator[In] => In],
    reduceFunction: Iterator[In] => Out)
    extends DataStream[Out] {

    override def createContract = {

      val keyFieldSelector = implicitly[FieldSelector[In => Key]]
      val keyFields = keyFieldSelector.getFields filter { _ >= 0 }
      val keyFieldTypes = implicitly[UDT[In]].getKeySet(keyFields)

      new ReduceContract(Reduce4sContract.getStub, keyFieldTypes, keyFields, input.getContract) with Reduce4sContract[Key, In, Out] {

        override val keySelector = keyFieldSelector
        override val inputUDT = implicitly[UDT[In]]
        override val outputUDT = implicitly[UDT[Out]]
        override val (combineUDF, reduceUDF) = reifyUDFs
        override val userCombineFunction = combineFunction
        override val userReduceFunction = reduceFunction

        def reifyUDFs: (UDF1[Iterator[In] => In], UDF1[Iterator[In] => Out]) = {

          val combineUDF = implicitly[UDF1[Iterator[In] => In]]
          val reduceUDF = implicitly[UDF1[Iterator[In] => Out]]

          if (combineUDF eq reduceUDF)
            (combineUDF, combineUDF.copy().asInstanceOf[UDF1[Iterator[In] => Out]])
          else
            (combineUDF, reduceUDF)
        }
      }
    }
  }
}
