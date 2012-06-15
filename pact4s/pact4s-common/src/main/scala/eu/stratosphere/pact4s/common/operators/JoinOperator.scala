package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class JoinOperator[LeftIn: UDT](leftInput: DataStream[LeftIn]) extends Serializable {

  def join[RightIn: UDT](rightInput: DataStream[RightIn]) = new Serializable {

    def on[Key, LeftKeySelector: SelectorBuilder[LeftIn, Key]#Selector](leftKeySelector: LeftIn => Key) = new Serializable {

      def isEqualTo[RightKeySelector: SelectorBuilder[RightIn, Key]#Selector](rightKeySelector: RightIn => Key) = new Serializable {

        def map[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](mapFunction: (LeftIn, RightIn) => Out) = createStream(Left(mapFunction))

        def flatMap[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Iterator[Out]]#UDF](mapFunction: (LeftIn, RightIn) => Iterator[Out]) = createStream(Right(mapFunction))

        private def createStream[Out: UDT, R, F: UDF2Builder[LeftIn, RightIn, R]#UDF](
          mapFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]): DataStream[Out] = new DataStream[Out] {

          override def createContract = {

            val leftKey = implicitly[FieldSelector[LeftIn => Key]]
            val rightKey = implicitly[FieldSelector[RightIn => Key]]
            val leftKeyFields = leftKey.getFields filter { _ >= 0 }
            val rightKeyFields = rightKey.getFields filter { _ >= 0 }
            val keyFieldTypes = implicitly[UDT[LeftIn]].getKeySet(leftKeyFields)

            new MatchContract(Join4sContract.getStub, keyFieldTypes, leftKeyFields, rightKeyFields, leftInput.getContract, rightInput.getContract) with Join4sContract[Key, LeftIn, RightIn, Out] {

              override val leftKeySelector = leftKey
              override val rightKeySelector = rightKey
              override val leftUDT = implicitly[UDT[LeftIn]]
              override val rightUDT = implicitly[UDT[RightIn]]
              override val outputUDT = implicitly[UDT[Out]]
              override val joinUDF = implicitly[UDF2[(LeftIn, RightIn) => R]]
              override val userFunction = mapFunction
            }
          }
        }
      }
    }
  }
}