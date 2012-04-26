package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.util._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.nephele.configuration.Configuration

import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.`type`.PactRecord

class Map4sStub extends MapStub with ParameterizedStub[MapParameters] {

  override def map(record: PactRecord, out: Collector) = {

    val MapParameters(deserializer, serializer, mapFunction) = getParameters
    
    val input = deserializer.deserialize(record)
    val output = mapFunction.apply(input)

    serializer.serialize(output, record)
    out.collect(record)
  }
}

class FlatMap4sStub extends MapStub with ParameterizedStub[FlatMapParameters] {

  override def map(record: PactRecord, out: Collector) = {

    val FlatMapParameters(deserializer, serializer, mapFunction) = getParameters

    val input = deserializer.deserialize(record)
    val output = mapFunction.apply(input)

    for (item <- output) {
      serializer.serialize(output, record)
      out.collect(record)
    }
  }
}