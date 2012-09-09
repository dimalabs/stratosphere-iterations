package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Union4sContract[T] extends NoOp4sContract { this: MapContract =>

  val udt: UDT[T]
  
  // Union is a no-op placeholder that reads nothing and writes nothing.
  // writeFields specifies the write location for the union's children
  // and the read location for its parent.
  val unionUDF: UDF1 = new AnalyzedUDF1 {
    override val readFields = getInitialReadFields(0)
    override val writeFields = getInitialWriteFields(udt.numFields)
  }

  override def annotations = Seq(
    Annotations.getConstantFields(unionUDF.getForwardedFields),
    Annotations.getOutCardBounds(Annotations.CARD_INPUTCARD, Annotations.CARD_INPUTCARD)
  /*
    Annotations.getReads(Array[Int]()),
    Annotations.getExplicitModifications(Array[Int]()),
    Annotations.getImplicitOperation(ImplicitOperationMode.Copy)
    */
  )
}

object Union4sContract {

  def newBuilder = MapContract.builder(classOf[NoOp4sStub])

  def unapply(c: Union4sContract[_]) = Some((c.getInputs(), c.udt, c.unionUDF))
}
