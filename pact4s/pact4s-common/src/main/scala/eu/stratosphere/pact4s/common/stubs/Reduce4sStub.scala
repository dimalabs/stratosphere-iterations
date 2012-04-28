package eu.stratosphere.pact4s.common.stubs

import java.util.{ Iterator => JIterator }

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable
import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.stubs.StubAnnotation._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode
import eu.stratosphere.pact.common.`type`.PactRecord

@ImplicitOperation(implicitOperation = ImplicitOperationMode.Projection)
class Reduce4sStub[In, Out] extends ReduceStub with ParameterizedStub[ReduceParameters[In, Out]] {

  private val outputRecord = new PactRecord()

  private var combineIterator: DeserializingIterator[In] = null
  private var combineForwardedFields: Array[Int] = _
  private var combineFunction: Iterator[In] => In = _
  private var combineSerializer: UDTSerializer[In] = _

  private var reduceIterator: DeserializingIterator[In] = null
  private var reduceForwardedFields: Array[Int] = _
  private var reduceFunction: Iterator[In] => Out = _
  private var reduceSerializer: UDTSerializer[Out] = _

  override def initialize(parameters: ReduceParameters[In, Out]) = {
    val ReduceParameters(inputUDT, outputUDT, combineUDF, combineFunction, reduceUDF, reduceFunction) = parameters

    this.combineIterator = combineUDF map { udf => new DeserializingIterator(inputUDT.createSerializer(udf.getReadFields)) } getOrElse null
    this.combineForwardedFields = combineUDF map { _.getForwardedFields.toArray } getOrElse null
    this.combineFunction = combineFunction getOrElse null
    this.combineSerializer = combineUDF map { udf => inputUDT.createSerializer(udf.getWriteFields) } getOrElse null

    this.reduceIterator = new DeserializingIterator(inputUDT.createSerializer(reduceUDF.getReadFields))
    this.reduceForwardedFields = reduceUDF.getForwardedFields.toArray
    this.reduceFunction = reduceFunction
    this.reduceSerializer = outputUDT.createSerializer(reduceUDF.getWriteFields)
  }

  override def combine(records: JIterator[PactRecord], out: Collector) = {

    combineIterator.initialize(records)

    val outputRecord = combineIterator.getFirstRecord
    val output = combineFunction.apply(combineIterator)

    outputRecord.copyFrom(combineIterator.getFirstRecord, combineForwardedFields, combineForwardedFields);

    combineSerializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }

  override def reduce(records: JIterator[PactRecord], out: Collector) = {

    reduceIterator.initialize(records)

    val output = reduceFunction.apply(reduceIterator)

    outputRecord.copyFrom(reduceIterator.getFirstRecord, reduceForwardedFields, reduceForwardedFields);

    reduceSerializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

@Combinable
@ImplicitOperation(implicitOperation = ImplicitOperationMode.Projection)
class CombinableReduce4sStub[In, Out] extends Reduce4sStub[In, Out]

