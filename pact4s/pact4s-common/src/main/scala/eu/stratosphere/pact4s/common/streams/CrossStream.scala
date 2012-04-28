package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class CrossStream[LeftIn: UDT, RightIn: UDT, Out: UDT, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapFunction: (LeftIn, RightIn) => Out)
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[Cross4sStub[LeftIn, RightIn, Out]]
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val mapUDF = implicitly[UDF2[(LeftIn, RightIn) => Out]]
    val name = getPactName getOrElse "<Unnamed Mapper>"

    new CrossContract(stub, leftInput.getContract, rightInput.getContract, name) with Cross4sContract[LeftIn, RightIn, Out] {

      override val stubParameters = new CrossParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

case class FlatCrossStream[LeftIn: UDT, RightIn: UDT, Out: UDT, F: UDF2Builder[LeftIn, RightIn, Iterator[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapFunction: (LeftIn, RightIn) => Iterator[Out])
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[FlatCross4sStub[LeftIn, RightIn, Out]]
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val mapUDF = implicitly[UDF2[(LeftIn, RightIn) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed Mapper>"

    new CrossContract(stub, leftInput.getContract, rightInput.getContract, name) with FlatCross4sContract[LeftIn, RightIn, Out] {

      override val stubParameters = new FlatCrossParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

