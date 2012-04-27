package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.stubs.StubAnnotation._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode
import eu.stratosphere.pact.common.`type`.PactRecord

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Copy)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Copy)
class Join4sStub[Key, LeftIn, RightIn, Out] extends MatchStub with ParameterizedStub[JoinParameters[Key, LeftIn, RightIn, Out]] {

  private val outputRecord = new PactRecord()

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var serializer: UDTSerializer[Out] = _
  private var mapFunction: (LeftIn, RightIn) => Out = _

  override def initialize(parameters: JoinParameters[Key, LeftIn, RightIn, Out]) = {
    val JoinParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.leftReadFields)
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.rightReadFields)
    this.serializer = outputUDT.createSerializer(mapUDF.writeFields)
    this.mapFunction = mapFunction
  }

  override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

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
class FlatJoin4sStub[Key, LeftIn, RightIn, Out] extends MatchStub with ParameterizedStub[FlatJoinParameters[Key, LeftIn, RightIn, Out]] {

  private val outputRecord = new PactRecord()

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var serializer: UDTSerializer[Out] = _
  private var mapFunction: (LeftIn, RightIn) => Iterator[Out] = _

  override def initialize(parameters: FlatJoinParameters[Key, LeftIn, RightIn, Out]) = {
    val FlatJoinParameters(leftUDT, leftKeySelector, rightUDT, rightKeySelector, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.leftReadFields)
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.rightReadFields)
    this.serializer = outputUDT.createSerializer(mapUDF.writeFields)
    this.mapFunction = mapFunction
  }

  override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

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