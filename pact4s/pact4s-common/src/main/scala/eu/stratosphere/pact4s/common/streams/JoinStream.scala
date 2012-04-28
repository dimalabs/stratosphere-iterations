package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class JoinStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (LeftIn, RightIn) => Out)
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[Join4sStub[LeftIn, RightIn, Out]]
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val mapUDF = implicitly[UDF2[(LeftIn, RightIn) => Out]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val leftKey = implicitly[KeySelector[LeftIn => Key]]
    val rightKey = implicitly[KeySelector[RightIn => Key]]

    new MatchContract(stub, leftKey.keyFieldTypes, leftKey.getKeyFields, rightKey.getKeyFields, leftInput.getContract, rightInput.getContract, name) with Join4sContract[Key, LeftIn, RightIn, Out] {

      override val leftKeySelector = leftKey
      override val rightKeySelector = rightKey
      override val stubParameters = new JoinParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

case class FlatJoinStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[LeftIn, RightIn, Iterator[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (LeftIn, RightIn) => Iterator[Out])
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[FlatJoin4sStub[LeftIn, RightIn, Out]]
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val mapUDF = implicitly[UDF2[(LeftIn, RightIn) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val leftKey = implicitly[KeySelector[LeftIn => Key]]
    val rightKey = implicitly[KeySelector[RightIn => Key]]

    new MatchContract(stub, leftKey.keyFieldTypes, leftKey.getKeyFields, rightKey.getKeyFields, leftInput.getContract, rightInput.getContract, name) with FlatJoin4sContract[Key, LeftIn, RightIn, Out] {

      override val leftKeySelector = leftKey
      override val rightKeySelector = rightKey
      override val stubParameters = new FlatJoinParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

