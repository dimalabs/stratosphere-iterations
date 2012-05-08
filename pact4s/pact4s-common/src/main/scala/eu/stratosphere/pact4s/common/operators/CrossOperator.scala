package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class CrossOperator[LeftIn: UDT](leftInput: DataStream[LeftIn]) extends Serializable {

  def cross[RightIn: UDT](rightInput: DataStream[RightIn]) = new Serializable {

    def map[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](mapFunction: (LeftIn, RightIn) => Out) = createStream(Left(mapFunction))

    def flatMap[Out: UDT, F: UDF2Builder[LeftIn, RightIn, Iterator[Out]]#UDF](mapFunction: (LeftIn, RightIn) => Iterator[Out]) = createStream(Right(mapFunction))

    private def createStream[Out: UDT, R, F: UDF2Builder[LeftIn, RightIn, R]#UDF](
      mapFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]): DataStream[Out] = new DataStream[Out] {

      override def createContract = {

        new CrossContract(Cross4sContract.getStub, leftInput.getContract, rightInput.getContract) with Cross4sContract[LeftIn, RightIn, Out] {

          override val leftUDT = implicitly[UDT[LeftIn]]
          override val rightUDT = implicitly[UDT[RightIn]]
          override val outputUDT = implicitly[UDT[Out]]
          override val crossUDF = implicitly[UDF2[(LeftIn, RightIn) => R]]
          override val userFunction = mapFunction
        }
      }
    }
  }
}

