package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

trait Pact4sContract { this: Contract =>
  def persistConfiguration() = {}
}

object Pact4sContract {
  implicit def toContract(c: Pact4sContract): Contract = c
}

trait ParameterizedContract[T <: StubParameters] { this: Pact4sContract =>

  val stubParameters: T

  override def persistConfiguration() {
    StubParameters.setValue(this, stubParameters)
  }
}

trait KeyedOneInputContract[Key, In] { this: Pact4sContract =>

  val keySelector: FieldSelector[In => Key]
}

trait KeyedTwoInputContract[Key, LeftIn, RightIn] { this: Pact4sContract =>

  val leftKeySelector: FieldSelector[LeftIn => Key]
  val rightKeySelector: FieldSelector[RightIn => Key]
}

trait CoGroup4sContract[Key, LeftIn, RightIn, Out] extends Pact4sContract
  with ParameterizedContract[CoGroupParameters[LeftIn, RightIn, Out]]
  with KeyedTwoInputContract[Key, LeftIn, RightIn] { this: CoGroupContract => }

trait FlatCoGroup4sContract[Key, LeftIn, RightIn, Out] extends Pact4sContract
  with ParameterizedContract[FlatCoGroupParameters[LeftIn, RightIn, Out]]
  with KeyedTwoInputContract[Key, LeftIn, RightIn] { this: CoGroupContract => }

trait Cross4sContract[LeftIn, RightIn, Out] extends Pact4sContract
  with ParameterizedContract[CrossParameters[LeftIn, RightIn, Out]] { this: CrossContract => }

trait FlatCross4sContract[LeftIn, RightIn, Out] extends Pact4sContract
  with ParameterizedContract[FlatCrossParameters[LeftIn, RightIn, Out]] { this: CrossContract => }

trait Join4sContract[Key, LeftIn, RightIn, Out] extends Pact4sContract
  with ParameterizedContract[JoinParameters[LeftIn, RightIn, Out]]
  with KeyedTwoInputContract[Key, LeftIn, RightIn] { this: MatchContract => }

trait FlatJoin4sContract[Key, LeftIn, RightIn, Out] extends Pact4sContract
  with ParameterizedContract[FlatJoinParameters[LeftIn, RightIn, Out]]
  with KeyedTwoInputContract[Key, LeftIn, RightIn] { this: MatchContract => }

trait Map4sContract[In, Out] extends Pact4sContract
  with ParameterizedContract[MapParameters[In, Out]] { this: MapContract => }

trait FlatMap4sContract[In, Out] extends Pact4sContract
  with ParameterizedContract[FlatMapParameters[In, Out]] { this: MapContract => }

trait Reduce4sContract[Key, In, Out] extends Pact4sContract
  with ParameterizedContract[ReduceParameters[In, Out]]
  with KeyedOneInputContract[Key, In] { this: ReduceContract => }

