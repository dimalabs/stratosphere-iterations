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

class Pact4sMapContract(val stream: MapStream[_, _, _])
  extends MapContract(classOf[Pact4sMapStub], stream.input.getContract, stream.getPactNameOrElse("<Unnamed Mapper>")) with Pact4sContract {

  val inputType = stream.input.udt
  val outputType = stream.udt
  val descriptor = stream.udf
  val mapper = stream.mapper
  
  stream.applyHintsToContract(this)
  
  override def persistConfiguration() = {
    this.setParameter("deserializer", inputType.createSerializer(descriptor.readFields))
    this.setParameter("serializer", outputType.createSerializer(descriptor.writeFields))
    this.setParameter("mapper", mapper)
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

class Pact4sFlatMapContract(val stream: FlatMapStream[_, _, _])
  extends MapContract(classOf[Pact4sFlatMapStub], stream.input.getContract, stream.getPactNameOrElse("<Unnamed Mapper>")) with Pact4sContract {

  val inputType = stream.input.udt
  val outputType = stream.udt
  val descriptor = stream.udf
  val mapper = stream.mapper

  stream.applyHintsToContract(this)

  override def persistConfiguration() = {
    this.setParameter("deserializer", inputType.createSerializer(descriptor.readFields))
    this.setParameter("serializer", outputType.createSerializer(descriptor.writeFields))
    this.setParameter("mapper", mapper)
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
