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
  private var leftDiscardedFields: Iterable[Int] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var rightDiscardedFields: Iterable[Int] = _
  private var mapFunction: (LeftIn, RightIn) => Out = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: CrossParameters[LeftIn, RightIn, Out]) {
    val CrossParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.getReadFields._1)
    this.leftDiscardedFields = mapUDF.getDiscardedFields._1
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.getReadFields._2)
    this.rightDiscardedFields = mapUDF.getDiscardedFields._2
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = mapFunction.apply(left, right)

    for (field <- leftDiscardedFields)
      leftRecord.setNull(field)

    for (field <- rightDiscardedFields)
      rightRecord.setNull(field)

    leftRecord.unionFields(rightRecord)

    serializer.serialize(output, leftRecord)
    out.collect(leftRecord)
  }
}

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Copy)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Copy)
class FlatCross4sStub[LeftIn, RightIn, Out] extends CrossStub with ParameterizedStub[FlatCrossParameters[LeftIn, RightIn, Out]] {

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var leftDiscardedFields: Iterable[Int] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var rightDiscardedFields: Iterable[Int] = _
  private var mapFunction: (LeftIn, RightIn) => Iterator[Out] = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: FlatCrossParameters[LeftIn, RightIn, Out]) {
    val FlatCrossParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.getReadFields._1)
    this.leftDiscardedFields = mapUDF.getDiscardedFields._1
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.getReadFields._2)
    this.rightDiscardedFields = mapUDF.getDiscardedFields._2
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def cross(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

    val left = leftDeserializer.deserialize(leftRecord)
    val right = rightDeserializer.deserialize(rightRecord)
    val output = mapFunction.apply(left, right)

    if (output.nonEmpty) {

      for (field <- leftDiscardedFields)
        leftRecord.setNull(field)

      for (field <- rightDiscardedFields)
        rightRecord.setNull(field)

      leftRecord.unionFields(rightRecord)

      for (item <- output) {
        serializer.serialize(item, leftRecord)
        out.collect(leftRecord)
      }
    }
  }
}
