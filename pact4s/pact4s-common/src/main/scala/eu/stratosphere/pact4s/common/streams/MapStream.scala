package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class MapStream[In: UDT, Out: UDT, F: UDF1Builder[In, Out]#UDF](
  input: DataStream[In],
  mapFunction: In => Out)
  extends DataStream[Out] {

  override def contract = {

    val inputType = implicitly[UDT[In]]
    val outputType = implicitly[UDT[Out]]
    val descriptor = implicitly[UDF1[In => Out]]
    val name = getPactName getOrElse "<Unnamed Mapper>"

    new MapContract(classOf[Map4sStub], input.getContract, name) with ParameterizedContract[MapParameters] {

      def getStubParameters = {
        val deserializer = inputType.createSerializer(descriptor.readFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)

        new MapParameters(deserializer, serializer, mapFunction.asInstanceOf[Any => Any])
      }
    }
  }
}

case class FlatMapStream[In: UDT, Out: UDT, F: UDF1Builder[In, Iterator[Out]]#UDF](
  input: DataStream[In],
  mapFunction: In => Iterator[Out])
  extends DataStream[Out] {

  override def contract = {

    val inputType = implicitly[UDT[In]]
    val outputType = implicitly[UDT[Out]]
    val descriptor = implicitly[UDF1[In => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed Mapper>"

    new MapContract(classOf[FlatMap4sStub], input.getContract, name) with ParameterizedContract[FlatMapParameters] {

      def getStubParameters = {
        val deserializer = inputType.createSerializer(descriptor.readFields)
        val serializer = outputType.createSerializer(descriptor.writeFields)

        new FlatMapParameters(deserializer, serializer, mapFunction.asInstanceOf[Any => Iterator[Any]])
      }
    }
  }
}

