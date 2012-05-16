package eu.stratosphere.pact4s.common.stubs

import java.util.{ Iterator => JIterator }

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class CoGroupParameters[LeftIn, RightIn, Out](
  val leftDeserializer: UDTSerializer[LeftIn],
  val leftForward: Array[Int],
  val rightDeserializer: UDTSerializer[RightIn],
  val rightForward: Array[Int],
  val serializer: UDTSerializer[Out],
  val userFunction: Either[(Iterator[LeftIn], Iterator[RightIn]) => Out, (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]])
  extends StubParameters

class CoGroup4sStub[LeftIn, RightIn, Out] extends CoGroupStub {

  private val outputRecord = new PactRecord()

  private var leftIterator: DeserializingIterator[LeftIn] = null
  private var leftForward: Array[Int] = _
  private var rightIterator: DeserializingIterator[RightIn] = null
  private var rightForward: Array[Int] = _
  private var serializer: UDTSerializer[Out] = _

  private var userFunction: (JIterator[PactRecord], JIterator[PactRecord], Collector[PactRecord]) => Unit = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[CoGroupParameters[LeftIn, RightIn, Out]](config)

    this.leftIterator = new DeserializingIterator(parameters.leftDeserializer)
    this.leftForward = parameters.leftForward
    this.rightIterator = new DeserializingIterator(parameters.rightDeserializer)
    this.rightForward = parameters.rightForward
    this.serializer = parameters.serializer

    this.userFunction = parameters.userFunction.fold(doCoGroup _, doFlatCoGroup _)
  }

  override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector[PactRecord]) = userFunction(leftRecords, rightRecords, out)

  private def doCoGroup(userFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out)(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector[PactRecord]) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = userFunction.apply(leftIterator, rightIterator)

    outputRecord.copyFrom(leftIterator.getFirstRecord, leftForward, leftForward);
    outputRecord.copyFrom(rightIterator.getFirstRecord, rightForward, rightForward);

    serializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }

  private def doFlatCoGroup(userFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out])(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector[PactRecord]) = {

    leftIterator.initialize(leftRecords)
    rightIterator.initialize(rightRecords)

    val output = userFunction.apply(leftIterator, rightIterator)

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
