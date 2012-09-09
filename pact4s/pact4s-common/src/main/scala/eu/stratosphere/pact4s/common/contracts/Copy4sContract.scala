package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Copy4sContract extends Pact4sOneInputContract { this: MapContract =>

  val copyUDF: UDF1

  override def annotations = Seq(
    Annotations.getConstantFields(copyUDF.getWriteFields ++ copyUDF.getDiscardedFields),
    Annotations.getOutCardBounds(Annotations.CARD_INPUTCARD, Annotations.CARD_INPUTCARD)
  /*
    Annotations.getReads(copyUDF.getReadFields),
    Annotations.getExplicitModifications(copyUDF.getWriteFields),
    Annotations.getImplicitOperation(ImplicitOperationMode.Copy),
    Annotations.getExplicitProjections(copyUDF.getDiscardedFields),
    */
  )

  override def persistConfiguration() = {

    val from = copyUDF.getReadFields
    val to = copyUDF.getWriteFields
    val discard = copyUDF.getDiscardedFields

    val stubParameters = new CopyParameters(from, to, discard)
    stubParameters.persist(this)
  }
}

object Copy4sContract {

  def newBuilder = MapContract.builder(classOf[Copy4sStub])

  def unapply(c: Copy4sContract) = Some((c.singleInput, c.copyUDF))
}
