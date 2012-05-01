package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait CoGroup4sContract[Key, LeftIn, RightIn, Out] extends Pact4sContract { this: CoGroupContract =>

  val leftKeySelector: FieldSelector[LeftIn => Key]
  val rightKeySelector: FieldSelector[RightIn => Key]
  val leftUDT: UDT[LeftIn]
  val rightUDT: UDT[RightIn]
  val outputUDT: UDT[Out]
  val coGroupUDF: UDF2[(Iterator[LeftIn], Iterator[RightIn]) => _]
  val userFunction: Either[(Iterator[LeftIn], Iterator[RightIn]) => Out, (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]

  override def persistConfiguration() = {

    val leftDeserializer = leftUDT.createSerializer(coGroupUDF.getReadFields._1)
    val leftForward = coGroupUDF.getForwardedFields._1.toArray
    val rightDeserializer = rightUDT.createSerializer(coGroupUDF.getReadFields._2)
    val rightForward = coGroupUDF.getForwardedFields._2.toArray
    val serializer = outputUDT.createSerializer(coGroupUDF.getWriteFields)

    val stubParameters = CoGroupParameters(leftDeserializer, leftForward, rightDeserializer, leftForward, serializer, userFunction)
    stubParameters.persist(this)
  }
}

object CoGroup4sContract {
  def unapply(c: CoGroup4sContract[_, _, _, _]) = Some((c.leftKeySelector, c.rightKeySelector, c.leftUDT, c.rightUDT, c.outputUDT, c.coGroupUDF))
}