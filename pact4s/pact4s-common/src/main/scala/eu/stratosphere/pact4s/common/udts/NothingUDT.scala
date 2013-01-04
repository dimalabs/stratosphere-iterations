package eu.stratosphere.pact4s.common.udts

import eu.stratosphere.pact4s.common.analysis._

import eu.stratosphere.pact.common.`type`.{ Value => PactValue }

final class NothingUDT extends UDT[Nothing] {
  override val fieldTypes = Array[Class[_ <: PactValue]]()
  override def createSerializer(indexMap: Array[Int]) = throw new UnsupportedOperationException("Cannot create UDTSerializer for type Nothing")
}