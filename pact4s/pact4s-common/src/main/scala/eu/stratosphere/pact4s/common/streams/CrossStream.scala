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

    val leftInputType = implicitly[UDT[LeftIn]]
    val rightInputType = implicitly[UDT[RightIn]]
    val outputType = implicitly[UDT[Out]]
    val descriptor = implicitly[UDF2[(LeftIn, RightIn) => Out]]
    val name = getPactName getOrElse "<Unnamed Crosser>"

    new CrossContract(classOf[Cross4sStub], leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[CrossParameters] {

      def getStubParameters = {
        val leftDeserializer = leftInputType.createSerializer(descriptor.leftReadFields)
        val rightDeserializer = rightInputType.createSerializer(descriptor.rightReadFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)

        new CrossParameters(leftDeserializer, rightDeserializer, serializer, mapFunction.asInstanceOf[(Any, Any) => Any])
      }
    }
  }
}

case class FlatCrossStream[LeftIn: UDT, RightIn: UDT, Out: UDT, F: UDF2Builder[LeftIn, RightIn, Iterator[Out]]#UDF](
  leftInput: DataStream[LeftIn],
  rightInput: DataStream[RightIn],
  mapFunction: (LeftIn, RightIn) => Iterator[Out])
  extends DataStream[Out] {

  override def contract = {

    val leftInputType = implicitly[UDT[LeftIn]]
    val rightInputType = implicitly[UDT[RightIn]]
    val outputType = implicitly[UDT[Out]]
    val descriptor = implicitly[UDF2[(LeftIn, RightIn) => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed Crosser>"

    new CrossContract(classOf[FlatCross4sStub], leftInput.getContract, rightInput.getContract, name) with ParameterizedContract[FlatCrossParameters] {

      def getStubParameters = {
        val leftDeserializer = leftInputType.createSerializer(descriptor.leftReadFields)
        val rightDeserializer = rightInputType.createSerializer(descriptor.rightReadFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)

        new FlatCrossParameters(leftDeserializer, rightDeserializer, serializer, mapFunction.asInstanceOf[(Any, Any) => Iterator[Any]])
      }
    }
  }
}