package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait MapOperator[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def map[Out: UDT, F: UDF1Builder[In, Out]#UDF](mapFunction: In => Out) = createStream(Left(mapFunction))

  def flatMap[Out: UDT, F: UDF1Builder[In, Iterator[Out]]#UDF](mapFunction: In => Iterator[Out]) = createStream(Right(mapFunction))

  def filter[F: UDF1Builder[In, Boolean]#UDF](predicate: In => Boolean) = input flatMap { x => if (predicate(x)) Iterator.single(x) else Iterator.empty }

  private def createStream[Out: UDT, R, F: UDF1Builder[In, R]#UDF](
    mapFunction: Either[In => Out, In => Iterator[Out]]): DataStream[Out] = new DataStream[Out] {

    override def contract = {
      val stub = classOf[Map4sStub[In, Out]]
      val name = getPactName getOrElse "<Unnamed Mapper>"

      new MapContract(stub, input.getContract, name) with Map4sContract[In, Out] {

        override val inputUDT = implicitly[UDT[In]]
        override val outputUDT = implicitly[UDT[Out]]
        override val mapUDF = implicitly[UDF1[In => R]]
        override val userFunction = mapFunction
      }
    }
  }
}

