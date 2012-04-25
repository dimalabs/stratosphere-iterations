package eu.stratosphere.pact4s.common.operators

import eu.stratosphere.pact4s.common.streams._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.util.ForEachAble

trait MapOperator[In] { this: WrappedDataStream[In] =>

  private val input = this.inner

  def map[Out: UDT, F: UDF1Builder[In, Out]#UDF](mapper: In => Out) = new MapStream(input, mapper)

  def flatMap[Out: UDT, F: UDF1Builder[In, ForEachAble[Out]]#UDF](mapper: In => ForEachAble[Out]) = new FlatMapStream(input, mapper)

  def filter[F: UDF1Builder[In, Boolean]#UDF](predicate: In => Boolean) = input flatMap { x => if (predicate(x)) Some(x) else None }
}

