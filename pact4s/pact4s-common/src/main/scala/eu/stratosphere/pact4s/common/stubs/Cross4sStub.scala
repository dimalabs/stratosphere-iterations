package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.stubs.StubAnnotation._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode
import eu.stratosphere.pact.common.`type`.PactRecord

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Copy)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Copy)
class Cross4sStub[LeftIn, RightIn, Out] extends CrossStub with ParameterizedStub[CrossParameters[LeftIn, RightIn, Out]] {

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var leftDiscard: Array[Int] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var rightDiscard: Array[Int] = _
  private var serializer: UDTSerializer[Out] = _

  private var stubFunction: (PactRecord, PactRecord, Collector) => Unit = _

  override def initialize(parameters: CrossParameters[LeftIn, RightIn, Out]) {

    this.leftDeserializer = parameters.leftDeserializer
    this.leftDiscard = parameters.leftDiscard
    this.rightDeserializer = parameters.rightDeserializer
    this.rightDiscard = parameters.rightDiscard
    this.serializer = parameters.serializer

    this.stubFunction = parameters.mapFunction.fold(doCross _, doFlatCross _)
  }

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) = stubFunction(leftRecord, rightRecord, out)

  private def doCross(mapFunction: (LeftIn, RightIn) => Out)(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = mapFunction.apply(left, right)

    for (field <- leftDiscard)
      leftRecord.setNull(field)

    for (field <- rightDiscard)
      rightRecord.setNull(field)

    leftRecord.unionFields(rightRecord)

    serializer.serialize(output, leftRecord)
    out.collect(leftRecord)
  }

  private def doFlatCross(mapFunction: (LeftIn, RightIn) => Iterator[Out])(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = mapFunction.apply(left, right)

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
