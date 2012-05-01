package eu.stratosphere.pact4s.common.contracts

import java.util.Collection

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait Pact4sContract { this: Contract =>
  def persistConfiguration() = {}
}

object Pact4sContract {
  implicit def toContract(c: Pact4sContract): Contract = c
}

trait Pact4sDataSinkContract extends Pact4sContract { this: GenericDataSink =>
}

object Pact4sDataSinkContract {
  implicit def toGenericSink(s: Pact4sDataSinkContract): GenericDataSink = s
  implicit def toGenericSinks(s: Seq[Pact4sDataSinkContract]): Collection[GenericDataSink] = s
}

trait KeyedOneInputContract[Key, In] { this: Pact4sContract =>

  val keySelector: FieldSelector[In => Key]
}

trait KeyedTwoInputContract[Key, LeftIn, RightIn] { this: Pact4sContract =>

  val leftKeySelector: FieldSelector[LeftIn => Key]
  val rightKeySelector: FieldSelector[RightIn => Key]
}
