package eu.stratosphere.pact4s.common.operators

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class UnionOperator[T: UDT](firstInput: DataStream[T]) extends Serializable {

  def union(secondInput: DataStream[T]) = new DataStream[T] {

    override def createContract = {

      val firstInputs = firstInput.getContract match {
        case Union4sContract(inputs) => inputs.toList
        case c                       => List(c)
      }

      val secondInputs = secondInput.getContract match {
        case Union4sContract(inputs) => inputs.toList
        case c                       => List(c)
      }

      val builder = Union4sContract.newBuilder.inputs(firstInputs ++ secondInputs)

      new MapContract(builder) with Union4sContract[T] {
        // Union is a no-op placeholder that reads nothing and writes nothing.
        // outputFields specifies the write location for the union's children
        // and the read location for its parent.
        override val udf = new UDF0[T]
      }
    }
  }
}