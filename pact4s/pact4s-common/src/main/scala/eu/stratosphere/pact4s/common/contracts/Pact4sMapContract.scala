package eu.stratosphere.pact4s.common.contracts

import scala.collection.GenTraversableOnce

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact4s.common.streams.FlatMapStream
import eu.stratosphere.pact4s.common.streams.MapStream

import eu.stratosphere.pact4s.common.util._

import eu.stratosphere.nephele.configuration.Configuration

import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.pact.common.`type`.PactRecord

object Pact4sMapContract {
  def createInstance[In: UDT, Out: UDT, F: UDF1Builder[In, Out]#UDF](stream: MapStream[In, Out, F]) = {
    val contract = new Pact4sMapContract(stream.input.getContract, stream.getPactNameOrElse("<Unnamed Mapper>"), stream.input.udt, stream.udt, stream.udf, stream.mapper)
    stream.applyHintsToContract(contract)
    contract
  }
}

class Pact4sMapContract(
  input: Pact4sContract,
  pactName: String,
  val inputType: UDT[_],
  val outputType: UDT[_],
  val descriptor: UDF1[_],
  val mapper: Function1[_, _])
  extends MapContract(classOf[Pact4sMapStub], input, pactName) with Pact4sContract {

  override def persistConfiguration() = {
    this.setParameter("deserializer", inputType.createSerializer(descriptor.readFields))
    this.setParameter("serializer", outputType.createSerializer(descriptor.writeFields))
    this.setParameter("mapper", mapper)

    input.persistConfiguration()
  }
}

class Pact4sMapStub extends MapStub {

  private var deserializer: UDTSerializer[Any] = null
  private var serializer: UDTSerializer[Any] = null
  private var mapper: Function1[Any, Any] = null

  override def open(config: Configuration) {
    deserializer = config.getObject("deserializer")
    serializer = config.getObject("serializer")
    mapper = config.getObject("mapper")
  }

  override def map(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = mapper.apply(input)

    serializer.serialize(output, record)
    out.collect(record)
  }
}

object Pact4sFlatMapContract {
  def createInstance[In: UDT, Out: UDT, F: UDF1Builder[In, GenTraversableOnce[Out]]#UDF](stream: FlatMapStream[In, Out, F]) = {
    val contract = new Pact4sFlatMapContract(stream.input.getContract, stream.getPactNameOrElse("<Unnamed Mapper>"), stream.input.udt, stream.udt, stream.udf, stream.mapper)
    stream.applyHintsToContract(contract)
    contract
  }
}

class Pact4sFlatMapContract(
  input: Pact4sContract,
  pactName: String,
  val inputType: UDT[_],
  val outputType: UDT[_],
  val descriptor: UDF1[_],
  val mapper: Function1[_, GenTraversableOnce[_]])
  extends MapContract(classOf[Pact4sFlatMapStub], input, pactName) with Pact4sContract {

  override def persistConfiguration() = {
    this.setParameter("deserializer", inputType.createSerializer(descriptor.readFields))
    this.setParameter("serializer", outputType.createSerializer(descriptor.writeFields))
    this.setParameter("mapper", mapper)

    input.persistConfiguration()
  }
}

class Pact4sFlatMapStub extends MapStub {

  private var deserializer: UDTSerializer[Any] = null
  private var serializer: UDTSerializer[Any] = null
  private var mapper: Function1[Any, GenTraversableOnce[Any]] = null

  override def open(config: Configuration) {
    deserializer = config.getObject("deserializer")
    serializer = config.getObject("serializer")
    mapper = config.getObject("mapper")
  }

  override def map(record: PactRecord, out: Collector) = {

    val input = deserializer.deserialize(record)
    val output = mapper.apply(input)

    for (item <- output) {
      serializer.serialize(output, record)
      out.collect(record)
    }
  }
}
