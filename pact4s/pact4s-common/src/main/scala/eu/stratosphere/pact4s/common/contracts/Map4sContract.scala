package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.util._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class Map4sContract(myName: Option[String], myInput: Contract, val mapFunction: _ => _, val inputType: UDT[_], val outputType: UDT[_], val descriptor: UDF1[_ => _])
  extends MapContract(classOf[Map4sStub], myInput, myName getOrElse "<Unnamed Mapper>") 
  with ParameterizedContract[MapParameters] {

  def getStubParameters = {
    val deserializer = inputType.createSerializer(descriptor.readFields)
    val serializer = outputType.createSerializer(descriptor.writeFields)

    new MapParameters(deserializer, serializer, mapFunction.asInstanceOf[Any => Any])
  }
}

case class FlatMap4sContract(myName: Option[String], myInput: Contract, val mapFunction: _ => ForEachAble[_], val inputType: UDT[_], val outputType: UDT[_], val descriptor: UDF1[_ => ForEachAble[_]])
  extends MapContract(classOf[FlatMap4sStub], myInput, myName getOrElse "<Unnamed Mapper>") 
  with ParameterizedContract[FlatMapParameters] {

  def getStubParameters = {
    val deserializer = inputType.createSerializer(descriptor.readFields)
    val serializer = outputType.createSerializer(descriptor.writeFields)

    new FlatMapParameters(deserializer, serializer, mapFunction.asInstanceOf[Any => ForEachAble[Any]])
  }
}