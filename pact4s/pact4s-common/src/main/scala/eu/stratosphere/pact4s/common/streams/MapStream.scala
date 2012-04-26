package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.util._

import eu.stratosphere.pact.common.contract._

case class MapStream[In: UDT, Out: UDT, F: UDF1Builder[In, Out]#UDF](
  input: DataStream[In],
  mapper: In => Out)
  extends DataStream[Out] {

  override def contract = new Map4sContract(this.getPactName, input.getContract, mapper, implicitly[UDT[In]], implicitly[UDT[Out]], implicitly[UDF1[In => Out]])
}

case class FlatMapStream[In: UDT, Out: UDT, F: UDF1Builder[In, ForEachAble[Out]]#UDF](
  input: DataStream[In],
  mapper: In => ForEachAble[Out])
  extends DataStream[Out] {

  override def contract = new FlatMap4sContract(this.getPactName, input.getContract, mapper, implicitly[UDT[In]], implicitly[UDT[Out]], implicitly[UDF1[In => ForEachAble[Out]]])
}

