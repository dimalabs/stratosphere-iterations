package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;

trait Join4sContract[Key, LeftIn, RightIn, Out] extends Pact4sContract { this: MatchContract =>

  def leftInput = this.getFirstInputs().get(0)
  def leftInput_=(left: Pact4sContract) = this.setFirstInput(left)

  def rightInput = this.getSecondInputs().get(0)
  def rightInput_=(right: Pact4sContract) = this.setSecondInput(right)

  val leftKeySelector: FieldSelector[LeftIn => Key]
  val rightKeySelector: FieldSelector[RightIn => Key]
  val leftUDT: UDT[LeftIn]
  val rightUDT: UDT[RightIn]
  val outputUDT: UDT[Out]
  val joinUDF: UDF2[(LeftIn, RightIn) => _]
  val userFunction: Either[(LeftIn, RightIn) => Out, (LeftIn, RightIn) => Iterator[Out]]

  override def annotations = Seq(
    new Annotations.ReadsFirst(joinUDF.getReadFields._1),
    new Annotations.ReadsSecond(joinUDF.getReadFields._2),
    new Annotations.ExplicitModifications(joinUDF.getWriteFields),
    new Annotations.ImplicitOperationFirst(ImplicitOperationMode.Copy),
    new Annotations.ImplicitOperationSecond(ImplicitOperationMode.Copy),
    new Annotations.ExplicitProjectionsFirst(joinUDF.getDiscardedFields._1),
    new Annotations.ExplicitProjectionsSecond(joinUDF.getDiscardedFields._2)
  )

  override def persistConfiguration() = {

    val leftDeserializer = leftUDT.createSerializer(joinUDF.getReadFields._1)
    val leftDiscard = joinUDF.getDiscardedFields._1
    val rightDeserializer = rightUDT.createSerializer(joinUDF.getReadFields._2)
    val rightDiscard = joinUDF.getDiscardedFields._2
    val serializer = outputUDT.createSerializer(joinUDF.getWriteFields)

    val stubParameters = new JoinParameters(leftDeserializer, leftDiscard, rightDeserializer, rightDiscard, serializer, userFunction)
    stubParameters.persist(this)
  }
}

object Join4sContract {

  def getStub[LeftIn, RightIn, Out] = classOf[Join4sStub[LeftIn, RightIn, Out]]

  def unapply(c: Join4sContract[_, _, _, _]) = Some((c.leftInput, c.rightInput, c.leftKeySelector, c.rightKeySelector, c.leftUDT, c.rightUDT, c.outputUDT, c.joinUDF))
}