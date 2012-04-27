package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

case class CoGroupStream[Key, LeftIn: UDT, RightIn: UDT, Out: UDT, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out)
  extends DataStream[Out] {

  override def contract = {
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val mapUDF = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Out]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val keyTypes = new Array[Class[_ <: PactKey]](0)

    new CoGroupContract(classOf[CoGroup4sStub[Key, LeftIn, RightIn, Out]], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[CoGroupParameters[Key, LeftIn, RightIn, Out]] {

      override val stubParameters = new CoGroupParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction)
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
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val mapUDF = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val keyTypes = new Array[Class[_ <: PactKey]](0)

    new CoGroupContract(classOf[FlatCoGroup4sStub[Key, LeftIn, RightIn, Out]], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[FlatCoGroupParameters[Key, LeftIn, RightIn, Out]] {

      override val stubParameters = new FlatCoGroupParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction)
    }
  }
}

