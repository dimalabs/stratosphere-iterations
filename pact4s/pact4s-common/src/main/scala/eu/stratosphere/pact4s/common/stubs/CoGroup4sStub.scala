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
  private var leftForward: Array[Int] = _
  private var rightIterator: DeserializingIterator[RightIn] = null
  private var rightForward: Array[Int] = _
  private var serializer: UDTSerializer[Out] = _

  private var stubFunction: (JIterator[PactRecord], JIterator[PactRecord], Collector) => Unit = _

  override def initialize(parameters: CoGroupParameters[LeftIn, RightIn, Out]) = {

    this.leftIterator = new DeserializingIterator(parameters.leftDeserializer)
    this.leftForward = parameters.leftForward
    this.rightIterator = new DeserializingIterator(parameters.rightDeserializer)
    this.rightForward = parameters.rightForward
    this.serializer = parameters.serializer

    this.stubFunction = parameters.mapFunction.fold(doCoGroup _, doFlatCoGroup _)
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = stubFunction(leftRecords, rightRecords, out)

  private def doCoGroup(mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out)(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    outputRecord.copyFrom(leftIterator.getFirstRecord, leftForward, leftForward);
    outputRecord.copyFrom(rightIterator.getFirstRecord, rightForward, rightForward);

    serializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }

  private def doFlatCoGroup(mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out])(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = mapFunction.apply(leftIterator, rightIterator)

    if (output.nonEmpty) {

      outputRecord.copyFrom(leftIterator.getFirstRecord, leftForward, leftForward);
      outputRecord.copyFrom(rightIterator.getFirstRecord, rightForward, rightForward);

      for (item <- output) {
        serializer.serialize(item, outputRecord)
        out.collect(outputRecord)
      }
    }
  }
}
