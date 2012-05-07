package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;

trait Cross4sContract[LeftIn, RightIn, Out] extends Pact4sTwoInputContract { this: CrossContract =>

  val leftUDT: UDT[LeftIn]
  val rightUDT: UDT[RightIn]
  val outputUDT: UDT[Out]
  val crossUDF: UDF2[(LeftIn, RightIn) => _]
  val userFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]

  override def annotations = Seq(
    new Annotations.ReadsFirst(crossUDF.getReadFields._1),
    new Annotations.ReadsSecond(crossUDF.getReadFields._2),
    new Annotations.ExplicitModifications(crossUDF.getWriteFields),
    new Annotations.ImplicitOperationFirst(ImplicitOperationMode.Copy),
    new Annotations.ImplicitOperationSecond(ImplicitOperationMode.Copy),
    new Annotations.ExplicitProjectionsFirst(crossUDF.getDiscardedFields._1),
    new Annotations.ExplicitProjectionsSecond(crossUDF.getDiscardedFields._2)
  )

  override def persistConfiguration() = {

    val leftDeserializer = leftUDT.createSerializer(crossUDF.getReadFields._1)
    val leftDiscard = crossUDF.getDiscardedFields._1
    val rightDeserializer = rightUDT.createSerializer(crossUDF.getReadFields._2)
    val rightDiscard = crossUDF.getDiscardedFields._2
    val serializer = outputUDT.createSerializer(crossUDF.getWriteFields)

    val stubParameters = new CrossParameters(leftDeserializer, leftDiscard, rightDeserializer, rightDiscard, serializer, userFunction)
    stubParameters.persist(this)
  }
}

object Cross4sContract {

  def getStub[LeftIn, RightIn, Out] = classOf[Cross4sStub[LeftIn, RightIn, Out]]

  def unapply(c: Cross4sContract[_, _, _]) = Some((c.leftInput, c.rightInput, c.leftUDT, c.rightUDT, c.outputUDT, c.crossUDF))
}