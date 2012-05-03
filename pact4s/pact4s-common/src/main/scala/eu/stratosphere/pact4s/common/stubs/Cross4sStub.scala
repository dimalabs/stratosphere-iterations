package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

case class CrossParameters[LeftIn, RightIn, Out](
  val leftDeserializer: UDTSerializer[LeftIn],
  val leftDiscard: Array[Int],
  val rightDeserializer: UDTSerializer[RightIn],
  val rightDiscard: Array[Int],
  val serializer: UDTSerializer[Out],
  val userFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]])
  extends StubParameters

class Cross4sStub[LeftIn, RightIn, Out] extends CrossStub {

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var leftDiscard: Array[Int] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var rightDiscard: Array[Int] = _
  private var serializer: UDTSerializer[Out] = _

  private var userFunction: (PactRecord, PactRecord, Collector) => Unit = _

  override def open(config: Configuration) = {
    super.open(config)
    val parameters = StubParameters.getValue[CrossParameters[LeftIn, RightIn, Out]](config)

    this.leftDeserializer = parameters.leftDeserializer
    this.leftDiscard = parameters.leftDiscard
    this.rightDeserializer = parameters.rightDeserializer
    this.rightDiscard = parameters.rightDiscard
    this.serializer = parameters.serializer

    this.userFunction = parameters.userFunction.fold(doCross _, doFlatCross _)
  }

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) = userFunction(leftRecord, rightRecord, out)

  private def doCross(userFunction: (LeftIn, RightIn) => Out)(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = userFunction.apply(left, right)

    for (field <- leftDiscard)
      leftRecord.setNull(field)

    for (field <- rightDiscard)
      rightRecord.setNull(field)

    leftRecord.unionFields(rightRecord)

    serializer.serialize(output, leftRecord)
    out.collect(leftRecord)
  }

  private def doFlatCross(userFunction: (LeftIn, RightIn) => Iterator[Out])(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = userFunction.apply(left, right)

    if (output.nonEmpty) {

      for (field <- leftDiscard)
        leftRecord.setNull(field)

      for (field <- rightDiscard)
        rightRecord.setNull(field)

      leftRecord.unionFields(rightRecord)

      for (item <- output) {
        serializer.serialize(item, leftRecord)
        out.collect(leftRecord)
      }
    }
  }
}
