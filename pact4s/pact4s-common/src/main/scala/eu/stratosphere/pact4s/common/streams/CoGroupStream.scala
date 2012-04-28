package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class CoGroupStream[Key, LeftIn: UDT, RightIn: UDT, Out: UDT, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out)
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[CoGroup4sStub[LeftIn, RightIn, Out]]
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val mapUDF = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Out]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val leftKey = implicitly[KeySelector[LeftIn => Key]]
    val rightKey = implicitly[KeySelector[RightIn => Key]]

    new CoGroupContract(stub, leftKey.keyFieldTypes, leftKey.getKeyFields, rightKey.getKeyFields, leftInput.getContract, rightInput.getContract, name) with CoGroup4sContract[Key, LeftIn, RightIn, Out] {

      override val leftKeySelector = leftKey
      override val rightKeySelector = rightKey
      override val stubParameters = new CoGroupParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

case class FlatCoGroupStream[Key, LeftIn: UDT, RightIn: UDT, Out: UDT, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Iterator[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out])
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[FlatCoGroup4sStub[LeftIn, RightIn, Out]]
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val mapUDF = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val leftKey = implicitly[KeySelector[LeftIn => Key]]
    val rightKey = implicitly[KeySelector[RightIn => Key]]

    new CoGroupContract(stub, leftKey.keyFieldTypes, leftKey.getKeyFields, rightKey.getKeyFields, leftInput.getContract, rightInput.getContract, name) with FlatCoGroup4sContract[Key, LeftIn, RightIn, Out] {

      override val leftKeySelector = leftKey
      override val rightKeySelector = rightKey
      override val stubParameters = new FlatCoGroupParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

