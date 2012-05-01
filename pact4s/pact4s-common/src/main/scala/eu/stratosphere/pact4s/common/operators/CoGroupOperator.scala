package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait CoGroupOperator[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def cogroup[RightIn: UDT](rightInput: DataStream[RightIn]) = new {

    def on[Key, LeftKeySelector: SelectorBuilder[LeftIn, Key]#Selector](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo[Key, RightKeySelector: SelectorBuilder[RightIn, Key]#Selector](rightKeySelector: RightIn => Key) = new {

        def map[Out: UDT, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Out]#UDF](mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out) = createStream(Left(mapFunction))

        def flatMap[Out: UDT, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Iterator[Out]]#UDF](mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]) = createStream(Right(mapFunction))

        private def createStream[Out: UDT, R, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], R]#UDF](
          mapFunction: Either[(Iterator[LeftIn], Iterator[RightIn]) => Out, (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]): DataStream[Out] = new DataStream[Out] {

          override def contract = {

            val stub = classOf[CoGroup4sStub[LeftIn, RightIn, Out]]
            val name = getPactName getOrElse "<Unnamed CoGrouper>"

            val leftKey = implicitly[FieldSelector[LeftIn => Key]]
            val rightKey = implicitly[FieldSelector[RightIn => Key]]
            val keyFieldTypes = implicitly[UDT[LeftIn]].getKeySet(leftKey.getFields)

            new CoGroupContract(stub, keyFieldTypes, leftKey.getFields, rightKey.getFields, leftInput.getContract, rightInput.getContract, name) with CoGroup4sContract[Key, LeftIn, RightIn, Out] {

              override val leftKeySelector = leftKey
              override val rightKeySelector = rightKey
              override val leftUDT = implicitly[UDT[LeftIn]]
              override val rightUDT = implicitly[UDT[RightIn]]
              override val outputUDT = implicitly[UDT[Out]]
              override val coGroupUDF = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => R]]
              override val userFunction = mapFunction
            }
          }
        }
      }
    }
  }
}
