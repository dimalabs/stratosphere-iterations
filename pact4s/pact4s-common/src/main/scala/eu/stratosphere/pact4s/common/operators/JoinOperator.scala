package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait JoinOperator[LeftIn] { this: WrappedDataStream[LeftIn] =>

  private val leftInput = this.inner

  def join[RightIn: UDT](rightInput: DataStream[RightIn]) = new {

    def on[Key, LeftKeySelector: SelectorBuilder[LeftIn, Key]#Selector](leftKeySelector: LeftIn => Key) = new {

      def isEqualTo[RightKeySelector: SelectorBuilder[RightIn, Key]#Selector](rightKeySelector: RightIn => Key) = new {

        def map[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](mapFunction: (LeftIn, RightIn) => Out) = createStream(Left(mapFunction))

        def flatMap[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Iterator[Out]]#UDF](mapFunction: (LeftIn, RightIn) => Iterator[Out]) = createStream(Right(mapFunction))

        private def createStream[Out: UDT, R, F: UDF2Builder[LeftIn, RightIn, R]#UDF](
          mapFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]): DataStream[Out] = new DataStream[Out] {

          override def contract = {

            val stub = classOf[Join4sStub[LeftIn, RightIn, Out]]
            val name = getPactName getOrElse "<Unnamed Matcher>"

            val leftKey = implicitly[FieldSelector[LeftIn => Key]]
            val rightKey = implicitly[FieldSelector[RightIn => Key]]
            val keyFieldTypes = implicitly[UDT[LeftIn]].getKeySet(leftKey.getFields)

            new MatchContract(stub, keyFieldTypes, leftKey.getFields, rightKey.getFields, leftInput.getContract, rightInput.getContract, name) with Join4sContract[Key, LeftIn, RightIn, Out] {

              val leftUDT = implicitly[UDT[LeftIn]]
              val rightUDT = implicitly[UDT[RightIn]]
              val outputUDT = implicitly[UDT[Out]]
              val mapUDF = implicitly[UDF2[(LeftIn, RightIn) => R]]

              override val leftKeySelector = leftKey
              override val rightKeySelector = rightKey

              override def getStubParameters = {

                val leftDeserializer = leftUDT.createSerializer(mapUDF.getReadFields._1)
                val leftDiscard = mapUDF.getDiscardedFields._1.toArray
                val rightDeserializer = rightUDT.createSerializer(mapUDF.getReadFields._2)
                val rightDiscard = mapUDF.getDiscardedFields._2.toArray
                val serializer = outputUDT.createSerializer(mapUDF.getWriteFields)

                new JoinParameters(leftDeserializer, leftDiscard, rightDeserializer, rightDiscard, serializer, mapFunction)
              }
            }
          }
        }
      }
    }
  }
}
