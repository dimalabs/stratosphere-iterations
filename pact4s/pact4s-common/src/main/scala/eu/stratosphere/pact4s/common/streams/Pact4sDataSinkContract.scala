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

trait FileDataSink4sContract[In] extends Pact4sDataSinkContract
  with ParameterizedContract[OutputParameters[In]] { this: FileDataSink => }

