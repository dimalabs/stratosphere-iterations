package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class CoGroupStream[Key, LeftIn: UDT, RightIn: UDT, Out: UDT, LeftKeySelector: SelectorBuilder[LeftIn, Key]#Selector, RightKeySelector: SelectorBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Out]#UDF](
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

    val leftKey = implicitly[FieldSelector[LeftIn => Key]]
    val rightKey = implicitly[FieldSelector[RightIn => Key]]
    val keyFieldTypes = leftUDT.getKeySet(leftKey.getFields)

    new CoGroupContract(stub, keyFieldTypes, leftKey.getFields, rightKey.getFields, leftInput.getContract, rightInput.getContract, name) with CoGroup4sContract[Key, LeftIn, RightIn, Out] {

      override val leftKeySelector = leftKey
      override val rightKeySelector = rightKey
      override val stubParameters = new CoGroupParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

case class FlatCoGroupStream[Key, LeftIn: UDT, RightIn: UDT, Out: UDT, LeftKeySelector: SelectorBuilder[LeftIn, Key]#Selector, RightKeySelector: SelectorBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Iterator[Out]]#UDF](
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
    val leftKeySelector = implicitly[FieldSelector[LeftIn => Key]]
    val rightKeySelector = implicitly[FieldSelector[RightIn => Key]]
    val mapUDF = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val leftKey = implicitly[FieldSelector[LeftIn => Key]]
    val rightKey = implicitly[FieldSelector[RightIn => Key]]
    val keyFieldTypes = leftUDT.getKeySet(leftKey.getFields)

    new CoGroupContract(stub, keyFieldTypes, leftKey.getFields, rightKey.getFields, leftInput.getContract, rightInput.getContract, name) with FlatCoGroup4sContract[Key, LeftIn, RightIn, Out] {

      override val leftKeySelector = leftKey
      override val rightKeySelector = rightKey
      override val stubParameters = new FlatCoGroupParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

