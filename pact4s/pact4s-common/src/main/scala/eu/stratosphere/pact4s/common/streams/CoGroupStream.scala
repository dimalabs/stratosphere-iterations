package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

case class CoGroupStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Out]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out)
  extends DataStream[Out] {

  override def contract = {
    val leftInputType = implicitly[UDT[LeftIn]]
    val rightInputType = implicitly[UDT[RightIn]]
    val outputType = implicitly[UDT[Out]]
    val keyTypes = new Array[Class[_ <: PactKey]](0)
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val descriptor = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Out]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    new CoGroupContract(classOf[CoGroup4sStub], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[CoGroupParameters] {

      def getStubParameters = {
        val leftDeserializer = leftInputType.createSerializer(descriptor.leftReadFields)
        val rightDeserializer = rightInputType.createSerializer(descriptor.rightReadFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)
        val leftCopyKeys = leftKeySelector.readFields
        val rightCopyKeys = rightKeySelector.readFields

        new CoGroupParameters(leftDeserializer, rightDeserializer, serializer, leftCopyKeys, rightCopyKeys, mapFunction.asInstanceOf[(Iterator[Any], Iterator[Any]) => Any])
      }
    }
  }
}

case class FlatCoGroupStream[LeftIn: UDT, RightIn: UDT, Out: UDT, Key, LeftKeySelector: KeyBuilder[LeftIn, Key]#Selector, RightKeySelector: KeyBuilder[RightIn, Key]#Selector, F: UDF2Builder[Iterator[LeftIn], Iterator[RightIn], Iterator[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  leftKeySelector: LeftIn => Key,
  rightKeySelector: RightIn => Key,
  mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out])
  extends DataStream[Out] {

  override def contract = {
    val leftInputType = implicitly[UDT[LeftIn]]
    val rightInputType = implicitly[UDT[RightIn]]
    val outputType = implicitly[UDT[Out]]
    val keyTypes = new Array[Class[_ <: PactKey]](0)
    val leftKeySelector = implicitly[KeySelector[LeftIn => Key]]
    val rightKeySelector = implicitly[KeySelector[RightIn => Key]]
    val descriptor = implicitly[UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed CoGrouper>"

    new CoGroupContract(classOf[FlatCoGroup4sStub], keyTypes, leftKeySelector.readFields, rightKeySelector.readFields, leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[FlatCoGroupParameters] {

      def getStubParameters = {
        val leftDeserializer = leftInputType.createSerializer(descriptor.leftReadFields)
        val rightDeserializer = rightInputType.createSerializer(descriptor.rightReadFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)
        val leftCopyKeys = leftKeySelector.readFields
        val rightCopyKeys = rightKeySelector.readFields

        new FlatCoGroupParameters(leftDeserializer, rightDeserializer, serializer, leftCopyKeys, rightCopyKeys, mapFunction.asInstanceOf[(Iterator[Any], Iterator[Any]) => Iterator[Any]])
      }
    }
  }
}