package eu.stratosphere.pact4s.common.operators

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

class UnionOperator[In: UDT](firstInput: DataStream[In]) extends Serializable {

  def union(secondInput: DataStream[In]) = new DataStream[In] {

    override def createContract = {

      val firstInputs = firstInput.getContract match {
        case Union4sContract(inputs, _, _) => inputs.toList
        case c                             => List(c)
      }

      val secondInputs = secondInput.getContract match {
        case Union4sContract(inputs, _, _) => inputs.toList
        case c                             => List(c)
      }

      val builder = Union4sContract.newBuilder.inputs(firstInputs ++ secondInputs)

      new MapContract(builder) with Union4sContract[In] {
        override val udt = implicitly[UDT[In]]
      }
    }
  }
}