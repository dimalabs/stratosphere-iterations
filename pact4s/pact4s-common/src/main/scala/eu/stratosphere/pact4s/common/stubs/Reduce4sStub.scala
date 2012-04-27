package eu.stratosphere.pact4s.common.stubs

import java.util.{ Iterator => JIterator }
import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.stubs._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable

class Reduce4sStub extends ReduceStub with ParameterizedStub[ReduceParameters] {

  override def reduce(records: JIterator[PactRecord], out: Collector) = {

  }

  override def combine(records: JIterator[PactRecord], out: Collector) = {

  }
}

@Combinable
class CombinableReduce4sStub extends Reduce4sStub

