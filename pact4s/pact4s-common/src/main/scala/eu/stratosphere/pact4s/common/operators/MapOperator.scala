package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._

trait MapOperator[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def map[Out: UDT, F: UDF1Builder[In, Out]#UDF](mapFunction: In => Out) = new MapStream(input, mapFunction)

  def flatMap[Out: UDT, F: UDF1Builder[In, Iterator[Out]]#UDF](mapFunction: In => Iterator[Out]) = new FlatMapStream(input, mapFunction)

  def filter[F: UDF1Builder[In, Boolean]#UDF](predicate: In => Boolean) = input flatMap { x => if (predicate(x)) Iterator.single(x) else Iterator.empty }
}

