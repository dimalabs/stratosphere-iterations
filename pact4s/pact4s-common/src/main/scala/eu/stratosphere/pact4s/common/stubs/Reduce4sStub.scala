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
class Reduce4sStub[Key, In, Out] extends ReduceStub with ParameterizedStub[ReduceParameters[Key, In, Out]] {

  private val outputRecord = new PactRecord()

  private var copyKeys: Array[Int] = _

  private var combineIterator: DeserializingIterator[In] = null
  private var combineSerializer: UDTSerializer[In] = _
  private var combineFunction: Iterator[In] => In = _

  private var reduceIterator: DeserializingIterator[In] = null
  private var reduceSerializer: UDTSerializer[Out] = _
  private var reduceFunction: Iterator[In] => Out = _

  override def initialize(parameters: ReduceParameters[Key, In, Out]) = {
    val ReduceParameters(inputUDT, outputUDT, keySelector, combineUDF, combineFunction, reduceUDF, reduceFunction) = parameters

    this.copyKeys = keySelector.readFields
    this.combineIterator = combineUDF map { udf => new DeserializingIterator(inputUDT.createSerializer(udf.readFields)) } getOrElse null
    this.combineSerializer = combineUDF map { udf => inputUDT.createSerializer(udf.writeFields) } getOrElse null
    this.combineFunction = combineFunction getOrElse null
    this.reduceIterator = new DeserializingIterator(inputUDT.createSerializer(reduceUDF.readFields))
    this.reduceSerializer = outputUDT.createSerializer(reduceUDF.writeFields)
    this.reduceFunction = reduceFunction
  }

  override def combine(records: JIterator[PactRecord], out: Collector) = {

    combineIterator.initialize(records)

    val outputRecord = combineIterator.getFirstRecord
    val output = combineFunction.apply(combineIterator)

    combineSerializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }

  override def reduce(records: JIterator[PactRecord], out: Collector) = {

    reduceIterator.initialize(records)

    val output = reduceFunction.apply(reduceIterator)

    outputRecord.copyFrom(reduceIterator.getFirstRecord, copyKeys, copyKeys);
    reduceSerializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

@Combinable
@ImplicitOperation(implicitOperation = ImplicitOperationMode.Projection)
class CombinableReduce4sStub[Key, In, Out] extends Reduce4sStub[Key, In, Out]

