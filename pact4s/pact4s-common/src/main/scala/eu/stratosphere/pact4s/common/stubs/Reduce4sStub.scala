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
  private var combineForward: Array[Int] = _
  private var combineFunction: Iterator[In] => In = _
  private var combineSerializer: UDTSerializer[In] = _

  private var reduceIterator: DeserializingIterator[In] = null
  private var reduceForward: Array[Int] = _
  private var reduceFunction: Iterator[In] => Out = _
  private var reduceSerializer: UDTSerializer[Out] = _

  override def initialize(parameters: ReduceParameters[In, Out]) = {

    this.combineIterator = parameters.combineDeserializer map { d => new DeserializingIterator(d) } getOrElse null
    this.combineForward = parameters.combineForward getOrElse null
    this.combineFunction = parameters.combineFunction getOrElse null
    this.combineSerializer = parameters.combineSerializer getOrElse null

    this.reduceIterator = new DeserializingIterator(parameters.reduceDeserializer)
    this.reduceForward = parameters.reduceForward
    this.reduceFunction = parameters.reduceFunction
    this.reduceSerializer = parameters.reduceSerializer
  }

  override def combine(records: JIterator[PactRecord], out: Collector) = {

    combineIterator.initialize(records)

    val outputRecord = combineIterator.getFirstRecord
    val output = combineFunction.apply(combineIterator)

    outputRecord.copyFrom(combineIterator.getFirstRecord, combineForward, combineForward);

    combineSerializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }

  override def reduce(records: JIterator[PactRecord], out: Collector) = {

    reduceIterator.initialize(records)

    val output = reduceFunction.apply(reduceIterator)

    outputRecord.copyFrom(reduceIterator.getFirstRecord, reduceForward, reduceForward);

    reduceSerializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

@Combinable
@ImplicitOperation(implicitOperation = ImplicitOperationMode.Projection)
class CombinableReduce4sStub[In, Out] extends Reduce4sStub[In, Out]

