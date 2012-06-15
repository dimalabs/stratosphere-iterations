package eu.stratosphere.pact4s.common

import scala.collection.TraversableOnce

import eu.stratosphere.pact4s.common.analyzer._

package object operators {

  implicit def dataSink2SinkToSource[T: UDT](sink: DataSink[T]): SinkToSourceOperator[T] = new SinkToSourceOperator(sink)
  implicit def dataStream2SourceToSink[T: UDT](input: DataStream[T]): SourceToSinkOperator[T] = new SourceToSinkOperator(input)

  implicit def dataStream2CoGroup[T: UDT](input: DataStream[T]): CoGroupOperator[T] = new CoGroupOperator(input)
  implicit def dataStream2Cross[T: UDT](input: DataStream[T]): CrossOperator[T] = new CrossOperator(input)
  implicit def dataStream2Join[T: UDT](input: DataStream[T]): JoinOperator[T] = new JoinOperator(input)
  implicit def dataStream2Map[T: UDT](input: DataStream[T]): MapOperator[T] = new MapOperator(input)
  implicit def dataStream2Reduce[T: UDT](input: DataStream[T]): ReduceOperator[T] = new ReduceOperator(input)

  implicit def funToRepeat[SolutionItem: UDT](stepFunction: DataStream[SolutionItem] => DataStream[SolutionItem]) = new RepeatOperator(stepFunction)
  implicit def funToIterate[SolutionItem: UDT, DeltaItem: UDT](stepFunction: DataStream[SolutionItem] => (DataStream[SolutionItem], DataStream[DeltaItem])) = new IterateOperator(stepFunction)
  implicit def funToWorksetIterate[SolutionItem: UDT, WorksetItem: UDT](stepFunction: (DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])) = new WorksetIterateOperator(stepFunction)

  implicit def dataStream2DistinctBy[T: UDT](input: DataStream[T]): DistinctByOperator[T] = new DistinctByOperator(input)

  implicit def traversableToIterator[T](i: TraversableOnce[T]): Iterator[T] = i.toIterator
  implicit def optionToIterator[T](opt: Option[T]): Iterator[T] = opt.iterator
  implicit def arrayToIterator[T](arr: Array[T]): Iterator[T] = arr.iterator
}