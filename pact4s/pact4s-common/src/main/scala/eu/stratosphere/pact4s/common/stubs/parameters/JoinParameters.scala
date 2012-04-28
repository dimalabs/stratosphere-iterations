package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class JoinParameters[LeftIn, RightIn, Out](
  val leftUDT: UDT[LeftIn],
  val rightUDT: UDT[RightIn],
  val outputUDT: UDT[Out],
  val mapUDF: UDF2[(LeftIn, RightIn) => Out],
  val mapFunction: (LeftIn, RightIn) => Out)
  extends StubParameters

case class FlatJoinParameters[LeftIn, RightIn, Out](
  val leftUDT: UDT[LeftIn],
  val rightUDT: UDT[RightIn],
  val outputUDT: UDT[Out],
  val mapUDF: UDF2[(LeftIn, RightIn) => Iterator[Out]],
  val mapFunction: (LeftIn, RightIn) => Iterator[Out])
  extends StubParameters