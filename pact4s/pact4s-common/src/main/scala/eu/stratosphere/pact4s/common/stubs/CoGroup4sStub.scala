package eu.stratosphere.pact4s.common.stubs

import java.util.{ Iterator => JIterator }
import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.Value

class CoGroup4sStub extends CoGroupStub with ParameterizedStub[CoGroupParameters] {

  private val outputRecord = new PactRecord()

  private var leftIterator: DeserializingIterator = null
  private var rightIterator: DeserializingIterator = null

  override def initialize() = {

    leftIterator = new DeserializingIterator(getParameters.leftDeserializer)
    rightIterator = new DeserializingIterator(getParameters.rightDeserializer)
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    val CoGroupParameters(_, _, serializer, leftCopyKeys, rightCopyKeys, mapFunction) = getParameters

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    outputRecord.copyFrom(leftIterator.getFirstRecord, leftCopyKeys, leftCopyKeys);
    outputRecord.copyFrom(rightIterator.getFirstRecord, rightCopyKeys, rightCopyKeys);

    serializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

class FlatCoGroup4sStub extends CoGroupStub with ParameterizedStub[FlatCoGroupParameters] {

  private val outputRecord = new PactRecord()

  private var leftIterator: DeserializingIterator = null
  private var rightIterator: DeserializingIterator = null

  override def initialize() = {

    leftIterator = new DeserializingIterator(getParameters.leftDeserializer)
    rightIterator = new DeserializingIterator(getParameters.rightDeserializer)
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    val FlatCoGroupParameters(_, _, serializer, leftCopyKeys, rightCopyKeys, mapFunction) = getParameters

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    if (!output.isEmpty) {

      outputRecord.copyFrom(leftIterator.getFirstRecord, leftCopyKeys, leftCopyKeys);
      outputRecord.copyFrom(rightIterator.getFirstRecord, rightCopyKeys, rightCopyKeys);

      for (item <- output) {
        serializer.serialize(output, outputRecord)
        out.collect(outputRecord)
      }
    }
  }
}
