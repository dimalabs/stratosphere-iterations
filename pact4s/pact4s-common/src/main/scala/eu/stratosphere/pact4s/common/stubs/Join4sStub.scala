package eu.stratosphere.pact4s.common.stubs

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.stubs.StubAnnotation._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode
import eu.stratosphere.pact.common.`type`.PactRecord

@ImplicitOperationFirst(implicitOperation = ImplicitOperationMode.Copy)
@ImplicitOperationSecond(implicitOperation = ImplicitOperationMode.Copy)
class Join4sStub[LeftIn, RightIn, Out] extends MatchStub with ParameterizedStub[JoinParameters[LeftIn, RightIn, Out]] {

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var leftDiscardedFields: Iterable[Int] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var rightDiscardedFields: Iterable[Int] = _
  private var mapFunction: (LeftIn, RightIn) => Out = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: JoinParameters[LeftIn, RightIn, Out]) = {
    val JoinParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.getReadFields._1)
    this.leftDiscardedFields = mapUDF.getDiscardedFields._1
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.getReadFields._2)
    this.rightDiscardedFields = mapUDF.getDiscardedFields._2
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

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
class FlatJoin4sStub[LeftIn, RightIn, Out] extends MatchStub with ParameterizedStub[FlatJoinParameters[LeftIn, RightIn, Out]] {

  private var leftDeserializer: UDTSerializer[LeftIn] = _
  private var leftDiscardedFields: Iterable[Int] = _
  private var rightDeserializer: UDTSerializer[RightIn] = _
  private var rightDiscardedFields: Iterable[Int] = _
  private var mapFunction: (LeftIn, RightIn) => Iterator[Out] = _
  private var serializer: UDTSerializer[Out] = _

  override def initialize(parameters: FlatJoinParameters[LeftIn, RightIn, Out]) = {
    val FlatJoinParameters(leftUDT, rightUDT, outputUDT, mapUDF, mapFunction) = parameters

    this.leftDeserializer = leftUDT.createSerializer(mapUDF.getReadFields._1)
    this.leftDiscardedFields = mapUDF.getDiscardedFields._1
    this.rightDeserializer = rightUDT.createSerializer(mapUDF.getReadFields._2)
    this.rightDiscardedFields = mapUDF.getDiscardedFields._2
    this.mapFunction = mapFunction
    this.serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
  }

  override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector) {

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
