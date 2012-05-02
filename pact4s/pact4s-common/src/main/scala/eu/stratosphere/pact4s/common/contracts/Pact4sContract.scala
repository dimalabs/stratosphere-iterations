package eu.stratosphere.pact4s.common.contracts

import java.util.Collection

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.io._

trait Pact4sContract { this: Contract =>
  def persistConfiguration() = {}
}

object Pact4sContract {
  implicit def toContract(c: Pact4sContract): Contract = c
}

trait Pact4sDataSourceContract extends Pact4sContract { this: GenericDataSource[_ <: InputFormat[_]] =>

  val outputUDT: UDT[_]
  val fieldSelector: FieldSelector[_]
}

trait Pact4sDataSinkContract extends Pact4sContract { this: GenericDataSink =>

  val inputUDT: UDT[_]
  val fieldSelector: FieldSelector[_]
}

object Pact4sDataSinkContract {
  implicit def toGenericSink(s: Pact4sDataSinkContract): GenericDataSink = s
  implicit def toGenericSinks(s: Seq[Pact4sDataSinkContract]): Collection[GenericDataSink] = s
}

