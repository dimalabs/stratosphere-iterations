package eu.stratosphere.pact4s.common.streams

import scala.collection.GenTraversableOnce

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact4s.common.contracts.Pact4sFlatMapContract
import eu.stratosphere.pact4s.common.contracts.Pact4sMapContract

trait MappableStream[In] { this: WrappedDataStream[In] =>

  private val input = this.inner
  
  def map[Out: UDT, F: UDF1Builder[In, Out]#UDF](mapper: In => Out) = new MapStream(input, mapper)

  def flatMap[Out: UDT, F: UDF1Builder[In, GenTraversableOnce[Out]]#UDF](mapper: In => GenTraversableOnce[Out]) = new FlatMapStream(input, mapper)

  def filter[F: UDF1Builder[In, Boolean]#UDF](predicate: In => Boolean) = input flatMap { x => if (predicate(x)) Some(x) else None }
}

case class MapStream[In: UDT, Out: UDT, F: UDF1Builder[In, Out]#UDF](
  input: DataStream[In],
  mapper: In => Out)
  extends DataStream[Out] {

  val udf = implicitly[UDF1[In => Out]]
  override def getContract = Pact4sMapContract.createInstance(this)
}

case class FlatMapStream[In: UDT, Out: UDT, F: UDF1Builder[In, GenTraversableOnce[Out]]#UDF](
  input: DataStream[In],
  mapper: In => GenTraversableOnce[Out])
  extends DataStream[Out] {

  val udf = implicitly[UDF1[In => GenTraversableOnce[Out]]]
  override def getContract = Pact4sFlatMapContract.createInstance(this)
}

