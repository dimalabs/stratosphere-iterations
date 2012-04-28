package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class MapStream[In: UDT, Out: UDT, F: UDF1Builder[In, Out]#UDF](
  input: DataStream[In],
  mapFunction: In => Out)
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[Map4sStub[In, Out]]
    val inputUDT = implicitly[UDT[In]]
    val outputUDT = implicitly[UDT[Out]]
    val mapUDF = implicitly[UDF1[In => Out]]
    val name = getPactName getOrElse "<Unnamed Mapper>"

    new MapContract(stub, input.getContract, name) with Map4sContract[In, Out] {

      override val stubParameters = new MapParameters(inputUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

case class FlatMapStream[In: UDT, Out: UDT, F: UDF1Builder[In, Iterator[Out]]#UDF](
  input: DataStream[In],
  mapFunction: In => Iterator[Out])
  extends DataStream[Out] {

  override def contract = {

    val stub = classOf[FlatMap4sStub[In, Out]]
    val inputUDT = implicitly[UDT[In]]
    val outputUDT = implicitly[UDT[Out]]
    val mapUDF = implicitly[UDF1[In => Iterator[Out]]]
    val name = getPactName getOrElse "<Unnamed Mapper>"

    new MapContract(stub, input.getContract, name) with FlatMap4sContract[In, Out] {

      override val stubParameters = new FlatMapParameters(inputUDT, outputUDT, mapUDF, mapFunction)
    }
  }
}

