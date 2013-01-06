package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Copy4sContract[In] extends Pact4sOneInputContract[In, In] { this: MapContract =>

  override def annotations = Seq(
    Annotations.getConstantFields(udf.getForwardIndexArray._1),
    Annotations.getOutCardBounds(Annotations.CARD_INPUTCARD, Annotations.CARD_INPUTCARD)
  /*
    Annotations.getReads(copyUDF.getReadFields),
    Annotations.getExplicitModifications(copyUDF.getWriteFields),
    Annotations.getImplicitOperation(ImplicitOperationMode.Copy),
    Annotations.getExplicitProjections(copyUDF.getDiscardedFields),
    */
  )

  override def persistConfiguration() = {

    val stubParameters = new CopyParameters(
      udf.inputFields.toSerializerIndexArray,
      udf.outputFields.toSerializerIndexArray,
      udf.inputFields.toFieldTypesArray(udf.inputUDT),
      udf.getDiscardIndexArray
    )
    stubParameters.persist(this)
  }
}

object Copy4sContract {

  def newBuilder = MapContract.builder(classOf[Copy4sStub])

  def apply[In](source: Pact4sContract[In]): Copy4sContract[In] = {
    new MapContract(Copy4sContract.newBuilder.input(source)) with Copy4sContract[In] {
      
      override val udf = new UDF1[In, In]()(source.getUDF.outputUDT, source.getUDF.outputUDT)
      
      this.setName("Copy " + source.getName())
      this.getCompilerHints().setAvgBytesPerRecord(source.getCompilerHints().getAvgBytesPerRecord())
    }
  }

  def unapply(c: Copy4sContract[_]) = Some(c.singleInput)
}
