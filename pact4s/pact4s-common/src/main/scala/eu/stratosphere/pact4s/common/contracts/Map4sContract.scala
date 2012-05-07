package eu.stratosphere.pact4s.common.contracts

import java.lang.annotation.Annotation

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;

trait Map4sContract[In, Out] extends Pact4sOneInputContract { this: MapContract =>

  val inputUDT: UDT[In]
  val outputUDT: UDT[Out]
  val mapUDF: UDF1[In => _]
  val userFunction: Either[In => Out, In => Iterator[Out]]

  override def annotations = Seq(
    new Annotations.Reads(mapUDF.getReadFields),
    new Annotations.ExplicitModifications(mapUDF.getWriteFields),
    new Annotations.ImplicitOperation(ImplicitOperationMode.Copy),
    new Annotations.ExplicitProjections(mapUDF.getDiscardedFields),
    new Annotations.OutCardBounds(outCardBound, outCardBound)
  )

  private val outCardBound = userFunction.fold({ _ => Annotations.OutCardBounds.INPUTCARD }, { _ => Annotations.OutCardBounds.UNKNOWN })

  override def persistConfiguration() = {

    val deserializer = inputUDT.createSerializer(mapUDF.getReadFields)
    val serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
    val discard = mapUDF.getDiscardedFields

    val stubParameters = new MapParameters(deserializer, serializer, discard, userFunction)
    stubParameters.persist(this)
  }
}

object Map4sContract {

  def getStub[In, Out] = classOf[Map4sStub[In, Out]]

  def unapply(c: Map4sContract[_, _]) = Some((c.singleInput, c.inputUDT, c.outputUDT, c.mapUDF))
}
