package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact4s.common.contracts.Pact4sFlatMapContract
import eu.stratosphere.pact4s.common.contracts.Pact4sMapContract

trait MappableStream[In] { this: WrappedDataStream[In] =>

  private val input = this.inner
  
  def map[Out: UDT, F: UDF1Builder[In, Out]#UDF](mapper: In => Out) = new MapStream(input, mapper)

  def flatMap[Out: UDT, F: UDF1Builder[In, ForEachAble[Out]]#UDF](mapper: In => ForEachAble[Out]) = new FlatMapStream(input, mapper)

  def filter[F: UDF1Builder[In, Boolean]#UDF](predicate: In => Boolean) = input flatMap { x => if (predicate(x)) Some(x) else None }
}

case class MapStream[In: UDT, Out: UDT, F: UDF1Builder[In, Out]#UDF](
  input: DataStream[In],
  mapper: In => Out)
  extends DataStream[Out] {

  val udf = implicitly[UDF1[In => Out]]
  override def getContract = new Pact4sMapContract(this)
}

case class FlatMapStream[In: UDT, Out: UDT, F: UDF1Builder[In, ForEachAble[Out]]#UDF](
  input: DataStream[In],
  mapper: In => ForEachAble[Out])
  extends DataStream[Out] {

  val udf = implicitly[UDF1[In => ForEachAble[Out]]]
  override def getContract = new Pact4sFlatMapContract(this)
}

