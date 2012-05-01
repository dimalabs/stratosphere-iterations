package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Cross4sContract[LeftIn, RightIn, Out] extends Pact4sContract { this: CrossContract =>

  val leftUDT: UDT[LeftIn]
  val rightUDT: UDT[RightIn]
  val outputUDT: UDT[Out]
  val crossUDF: UDF2[(LeftIn, RightIn) => _]
  val userFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]

  override def persistConfiguration() = {

    val leftDeserializer = leftUDT.createSerializer(crossUDF.getReadFields._1)
    val leftDiscard = crossUDF.getDiscardedFields._1.toArray
    val rightDeserializer = rightUDT.createSerializer(crossUDF.getReadFields._2)
    val rightDiscard = crossUDF.getDiscardedFields._2.toArray
    val serializer = outputUDT.createSerializer(crossUDF.getWriteFields)

    val stubParameters = new CrossParameters(leftDeserializer, leftDiscard, rightDeserializer, rightDiscard, serializer, userFunction)
    stubParameters.persist(this)
  }
}

object Cross4sContract {
  def unapply(c: Cross4sContract[_, _, _]) = Some((c.leftUDT, c.rightUDT, c.outputUDT, c.crossUDF))
}