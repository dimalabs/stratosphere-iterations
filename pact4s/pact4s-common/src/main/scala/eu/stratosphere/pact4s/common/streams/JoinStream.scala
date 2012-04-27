package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

case class JoinStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[LeftIn, RightIn, Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (LeftIn, RightIn) => Out)
  extends DataStream[Out] {

  override def contract = {
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val mapUDF = implicitly[UDF2[(LeftIn, RightIn) => Out]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val keyTypes = new Array[Class[_ <: PactKey]](0)

    new MatchContract(classOf[Join4sStub[Key, LeftIn, RightIn, Out]], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[JoinParameters[Key, LeftIn, RightIn, Out]] {

      override val stubParameters = new JoinParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction)
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
    val leftUDT = implicitly[UDT[LeftIn]]
    val rightUDT = implicitly[UDT[RightIn]]
    val outputUDT = implicitly[UDT[Out]]
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val mapUDF = implicitly[UDF2[(LeftIn, RightIn) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    val keyTypes = new Array[Class[_ <: PactKey]](0)

    new MatchContract(classOf[FlatJoin4sStub[Key, LeftIn, RightIn, Out]], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[FlatJoinParameters[Key, LeftIn, RightIn, Out]] {

      override val stubParameters = new FlatJoinParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction)
    }
  }
}