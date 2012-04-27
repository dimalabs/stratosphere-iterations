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
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var serializer: UDTSerializer[Out] = _
  private var mapFunction: (LeftIn, RightIn) => Out = _

  override def initialize(parameters: CrossParameters[LeftIn, RightIn, Out]) {
    val CrossParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.leftReadFields)
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.rightReadFields)
    this.serializer = outputUDT.createSerializer(mapUDF.writeFields)
    this.mapFunction = mapFunction
  }

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = mapFunction.apply(left, right)

    leftRecord.unionFields(rightRecord)
    serializer.serialize(output, leftRecord)
    out.collect(leftRecord)
  }
}

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Copy)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Copy)
class FlatCross4sStub[LeftIn, RightIn, Out] extends CrossStub with ParameterizedStub[FlatCrossParameters[LeftIn, RightIn, Out]] {

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var serializer: UDTSerializer[Out] = _
  private var mapFunction: (LeftIn, RightIn) => Iterator[Out] = _

  override def initialize(parameters: FlatCrossParameters[LeftIn, RightIn, Out]) {
    val FlatCrossParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.leftReadFields)
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.rightReadFields)
    this.serializer = outputUDT.createSerializer(mapUDF.writeFields)
    this.mapFunction = mapFunction
  }

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = mapFunction.apply(left, right)

    if (output.nonEmpty) {
      leftRecord.unionFields(rightRecord)

      for (item <- output) {
        serializer.serialize(item, leftRecord)
        out.collect(leftRecord)
      }
    }
  }
}