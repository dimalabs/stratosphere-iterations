package eu.stratosphere.pact4s.common.stubs.parameters

import eu.stratosphere.pact4s.common.analyzer._

case class CoGroupParameters[Key, LeftIn, RightIn, Out](
  val leftUDT: UDT[LeftIn],
  val leftKeySelector: KeySelector[LeftIn => Key],
  val rightUDT: UDT[RightIn],
  val rightKeySelector: KeySelector[RightIn => Key],
  val outputUDT: UDT[Out],
  val mapUDF: UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Out],
  val mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Out)
  extends StubParameters

case class FlatCoGroupParameters[Key, LeftIn, RightIn, Out](
  val leftUDT: UDT[LeftIn],
  val leftKeySelector: KeySelector[LeftIn => Key],
  val rightUDT: UDT[RightIn],
  val rightKeySelector: KeySelector[RightIn => Key],
  val outputUDT: UDT[Out],
  val mapUDF: UDF2[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]],
  val mapFunction: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out])
  extends StubParameters