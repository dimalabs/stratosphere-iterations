package eu.stratosphere.pact4s.common.streams

import java.util.Collection

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait Pact4sDataSinkContract extends Pact4sContract { this: GenericDataSink =>
}

object Pact4sDataSinkContract {
  implicit def toGenericSink(s: Pact4sDataSinkContract): GenericDataSink = s
  implicit def toGenericSinks(s: Seq[Pact4sDataSinkContract]): Collection[GenericDataSink] = s
}

trait RawDataSink4sContract[In] extends Pact4sDataSinkContract
  with ParameterizedContract[RawOutputParameters[In]] { this: FileDataSink => }

trait BinaryDataSink4sContract[In] extends Pact4sDataSinkContract
  with ParameterizedContract[BinaryOutputParameters[In]] { this: FileDataSink => }

trait SequentialDataSink4sContract extends Pact4sDataSinkContract { this: FileDataSink => }

trait DelimetedDataSink4sContract[In] extends Pact4sDataSinkContract
  with ParameterizedContract[DelimetedOutputParameters[In]] { this: FileDataSink => }

trait RecordDataSink4sContract extends Pact4sDataSinkContract { this: FileDataSink => }
