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
class CoGroup4sStub[LeftIn, RightIn, Out] extends CoGroupStub with ParameterizedStub[CoGroupParameters[LeftIn, RightIn, Out]] {

  private val outputRecord = new PactRecord()

  private var leftIterator: DeserializingIterator[LeftIn] = null
  private var leftForwardedFields: Array[Int] = _
  private var rightIterator: DeserializingIterator[RightIn] = null
  private var rightForwardedFields: Array[Int] = _
  private var mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: CoGroupParameters[LeftIn, RightIn, Out]) = {
    val CoGroupParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftIterator = new DeserializingIterator(leftUDT.createSerializer(mapUDF.getReadFields._1))
    this.leftForwardedFields = mapUDF.getForwardedFields._1.toArray
    this.rightIterator = new DeserializingIterator(rightUDT.createSerializer(mapUDF.getReadFields._1))
    this.rightForwardedFields = mapUDF.getForwardedFields._2.toArray
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    outputRecord.copyFrom(leftIterator.getFirstRecord, leftForwardedFields, leftForwardedFields);
    outputRecord.copyFrom(rightIterator.getFirstRecord, rightForwardedFields, rightForwardedFields);

    serializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Projection)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Projection)
class FlatCoGroup4sStub[LeftIn, RightIn, Out] extends CoGroupStub with ParameterizedStub[FlatCoGroupParameters[LeftIn, RightIn, Out]] {

  private val outputRecord = new PactRecord()

  private var leftIterator: DeserializingIterator[LeftIn] = null
  private var leftForwardedFields: Array[Int] = _
  private var rightIterator: DeserializingIterator[RightIn] = null
  private var rightForwardedFields: Array[Int] = _
  private var mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out] = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: FlatCoGroupParameters[LeftIn, RightIn, Out]) = {
    val FlatCoGroupParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftIterator = new DeserializingIterator(leftUDT.createSerializer(mapUDF.getReadFields._1))
    this.leftForwardedFields = mapUDF.getForwardedFields._1.toArray
    this.rightIterator = new DeserializingIterator(rightUDT.createSerializer(mapUDF.getReadFields._1))
    this.rightForwardedFields = mapUDF.getForwardedFields._2.toArray
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    if (output.nonEmpty) {

      outputRecord.copyFrom(leftIterator.getFirstRecord, leftForwardedFields, leftForwardedFields);
      outputRecord.copyFrom(rightIterator.getFirstRecord, rightForwardedFields, rightForwardedFields);

      for (item <- output) {
        serializer.serialize(item, outputRecord)
        out.collect(outputRecord)
      }
    }
  }
}
