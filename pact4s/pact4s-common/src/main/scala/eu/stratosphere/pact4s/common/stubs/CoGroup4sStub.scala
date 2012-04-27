package eu.stratosphere.pact4s.common.stubs

import java.util.{ Iterator => JIterator }

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.stubs.StubAnnotation._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode
import eu.stratosphere.pact.common.`type`.PactRecord

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Projection)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Projection)
class CoGroup4sStub[Key, LeftIn, RightIn, Out] extends CoGroupStub with ParameterizedStub[CoGroupParameters[Key, LeftIn, RightIn, Out]] {

  private val outputRecord = new PactRecord()

  private var leftIterator: DeserializingIterator[LeftIn] = null
  private var leftCopyKeys: Array[Int] = _
  private var rightIterator: DeserializingIterator[RightIn] = null
  private var rightCopyKeys: Array[Int] = _
  private var serializer: UDTSerializer[Out] = _
  private var mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out = _

  override def initialize(parameters: CoGroupParameters[Key, LeftIn, RightIn, Out]) = {
    val CoGroupParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction) = parameters

    this.leftIterator = new DeserializingIterator(leftUDT.createSerializer(mapUDF.leftReadFields))
    this.leftCopyKeys = leftKeySelector.readFields
    this.rightIterator = new DeserializingIterator(rightUDT.createSerializer(mapUDF.rightReadFields))
    this.rightCopyKeys = rightKeySelector.readFields
    this.serializer = outputUDT.createSerializer(mapUDF.writeFields)
    this.mapFunction = mapFunction
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    outputRecord.copyFrom(leftIterator.getFirstRecord, leftCopyKeys, leftCopyKeys);
    outputRecord.copyFrom(rightIterator.getFirstRecord, rightCopyKeys, rightCopyKeys);

    serializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Projection)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Projection)
class FlatCoGroup4sStub[Key, LeftIn, RightIn, Out] extends CoGroupStub with ParameterizedStub[FlatCoGroupParameters[Key, LeftIn, RightIn, Out]] {

  private val outputRecord = new PactRecord()

  private var leftIterator: DeserializingIterator[LeftIn] = null
  private var leftCopyKeys: Array[Int] = _
  private var rightIterator: DeserializingIterator[RightIn] = null
  private var rightCopyKeys: Array[Int] = _
  private var serializer: UDTSerializer[Out] = _
  private var mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out] = _

  override def initialize(parameters: FlatCoGroupParameters[Key, LeftIn, RightIn, Out]) = {
    val FlatCoGroupParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction) = parameters

    this.leftIterator = new DeserializingIterator(leftUDT.createSerializer(mapUDF.leftReadFields))
    this.leftCopyKeys = leftKeySelector.readFields
    this.rightIterator = new DeserializingIterator(rightUDT.createSerializer(mapUDF.rightReadFields))
    this.rightCopyKeys = rightKeySelector.readFields
    this.serializer = outputUDT.createSerializer(mapUDF.writeFields)
    this.mapFunction = mapFunction
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    if (output.nonEmpty) {

      outputRecord.copyFrom(leftIterator.getFirstRecord, leftCopyKeys, leftCopyKeys);
      outputRecord.copyFrom(rightIterator.getFirstRecord, rightCopyKeys, rightCopyKeys);

      for (item <- output) {
        serializer.serialize(output, outputRecord)
        out.collect(outputRecord)
      }
    }
  }
}
