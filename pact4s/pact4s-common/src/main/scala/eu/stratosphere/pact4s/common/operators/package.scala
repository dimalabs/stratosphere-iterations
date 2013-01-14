/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.pact4s.common

import scala.collection.TraversableOnce

import eu.stratosphere.pact4s.common.analysis._

package object operators {

  implicit def dataSink2SinkToSource[T: UDT](sink: DataSink[T]): SinkToSourceOperator[T] = new SinkToSourceOperator(sink)
  implicit def dataStream2SourceToSink[T: UDT](input: DataStream[T]): SourceToSinkOperator[T] = new SourceToSinkOperator(input)

  implicit def dataStream2CoGroup[T: UDT](input: DataStream[T]): CoGroupOperator[T] = new CoGroupOperator(input)
  implicit def dataStream2Cross[T: UDT](input: DataStream[T]): CrossOperator[T] = new CrossOperator(input)
  implicit def dataStream2Join[T: UDT](input: DataStream[T]): JoinOperator[T] = new JoinOperator(input)
  implicit def dataStream2Map[T: UDT](input: DataStream[T]): MapOperator[T] = new MapOperator(input)
  implicit def dataStream2Reduce[T: UDT](input: DataStream[T]): ReduceOperator[T] = new ReduceOperator(input)
  implicit def dataStream2Union[T: UDT](input: DataStream[T]): UnionOperator[T] = new UnionOperator(input)

  implicit def funToRepeat[SolutionItem: UDT](stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]) = new RepeatOperator(stepFunction)
  implicit def funToIterate[SolutionItem: UDT, DeltaItem: UDT](stepFunction: DataStream[SolutionItem] => (DataStream[SolutionItem], DataStream[DeltaItem])) = new IterateOperator(stepFunction)
  implicit def funToWorksetIterate[SolutionItem: UDT, WorksetItem: UDT](stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) = new WorksetIterateOperator(stepFunction)

  implicit def dataStream2DistinctBy[T: UDT](input: DataStream[T]): DistinctByOperator[T] = new DistinctByOperator(input)

  implicit def traversableToIterator[T](i: TraversableOnce[T]): Iterator[T] = i.toIterator
  implicit def optionToIterator[T](opt: Option[T]): Iterator[T] = opt.iterator
  implicit def arrayToIterator[T](arr: Array[T]): Iterator[T] = arr.iterator
}