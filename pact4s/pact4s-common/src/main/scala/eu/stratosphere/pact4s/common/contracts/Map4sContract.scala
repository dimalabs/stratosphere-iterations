package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Map4sContract[In, Out] extends Pact4sContract { this: MapContract =>

  val inputUDT: UDT[In]
  val outputUDT: UDT[Out]
  val mapUDF: UDF1[In => _]
  val userFunction: Either[In => Out, In => Iterator[Out]]

  override def persistConfiguration() = {

    val deserializer = inputUDT.createSerializer(mapUDF.getReadFields)
    val serializer = outputUDT.createSerializer(mapUDF.getWriteFields)
    val discard = mapUDF.getDiscardedFields.toArray

    val stubParameters = new MapParameters(deserializer, serializer, discard, userFunction)
    stubParameters.persist(this)
  }
}

object Map4sContract {
  def unapply(c: Map4sContract[_, _]) = Some((c.inputUDT, c.outputUDT, c.mapUDF))
}