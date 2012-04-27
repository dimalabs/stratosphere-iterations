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
    val leftInputType = implicitly[UDT[LeftIn]]
    val rightInputType = implicitly[UDT[RightIn]]
    val outputType = implicitly[UDT[Out]]
    val keyTypes = new Array[Class[_ <: PactKey]](0)
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val descriptor = implicitly[UDF2[(LeftIn, RightIn) => Out]]
    val name = getPactName getOrElse "<Unnamed Joiner>"

    new MatchContract(classOf[Join4sStub], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[JoinParameters] {

      def getStubParameters = {
        val leftDeserializer = leftInputType.createSerializer(descriptor.leftReadFields)
        val rightDeserializer = rightInputType.createSerializer(descriptor.rightReadFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)
        val leftCopyKeys = leftKeySelector.readFields
        val rightCopyKeys = rightKeySelector.readFields

        new JoinParameters(leftDeserializer, rightDeserializer, serializer, leftCopyKeys, rightCopyKeys, mapFunction.asInstanceOf[(Any, Any) => Any])
      }
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
    val leftInputType = implicitly[UDT[LeftIn]]
    val rightInputType = implicitly[UDT[RightIn]]
    val outputType = implicitly[UDT[Out]]
    val keyTypes = new Array[Class[_ <: PactKey]](0)
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val descriptor = implicitly[UDF2[(LeftIn, RightIn) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed Joiner>"

    new MatchContract(classOf[Join4sStub], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[JoinParameters] {

      def getStubParameters = {
        val leftDeserializer = leftInputType.createSerializer(descriptor.leftReadFields)
        val rightDeserializer = rightInputType.createSerializer(descriptor.rightReadFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)
        val leftCopyKeys = leftKeySelector.readFields
        val rightCopyKeys = rightKeySelector.readFields

        new JoinParameters(leftDeserializer, rightDeserializer, serializer, leftCopyKeys, rightCopyKeys, mapFunction.asInstanceOf[(Any, Any) => Iterator[Any]])
      }
    }
  }
}