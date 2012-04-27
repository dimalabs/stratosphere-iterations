package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

case class ReduceStream[Key, In: UDT, Out: UDT, GroupByKeySelector: KeyBuilder[In, Key]#Selector, FC: UDF1Builder[Iterator[In], In]#UDF, FR: UDF1Builder[Iterator[In], Out]#UDF](
  input: DataStream[In],
  keySelector: In => Key,
  combineFunction: Option[Iterator[In] => In],
  reduceFunction: Iterator[In] => Out)
  extends DataStream[Out] {

  override def contract = {

    val stub = combineFunction map { _ => classOf[CombinableReduce4sStub] } getOrElse classOf[Reduce4sStub]
    val inputType = implicitly[UDT[In]]
    val outputType = implicitly[UDT[Out]]
    val keyTypes = new Array[Class[_ <: PactKey]](0)
    val keySelector = implicitly[KeySelector[In => Key]]
    val combinerDescriptor = implicitly[UDF1[In => Out]]
    val reducerDescriptor = implicitly[UDF1[Iterator[In] => Out]]
    val name = getPactName getOrElse "<Unnamed Reducer>"

    new ReduceContract(stub, keyTypes, keySelector.readFields, input.getContract, name) with ParameterizedContract[ReduceParameters] {

      def getStubParameters = {
        val combinerSerializer = combineFunction map { _ => inputType.createSerializer(combinerDescriptor.readFields).asInstanceOf[UDTSerializer[Any]] }
        val deserializer = inputType.createSerializer(reducerDescriptor.readFields)
        val serializer = outputType.createSerializer(reducerDescriptor.writeFields)
        val copyKeys = keySelector.readFields
        val combineFunctionAny = combineFunction map { _.asInstanceOf[Iterator[Any] => Any] }
        val reduceFunctionAny = reduceFunction.asInstanceOf[Iterator[Any] => Any]

        new ReduceParameters(combinerSerializer, deserializer, serializer, copyKeys, combineFunctionAny, reduceFunctionAny)
      }
    }
  }
}
