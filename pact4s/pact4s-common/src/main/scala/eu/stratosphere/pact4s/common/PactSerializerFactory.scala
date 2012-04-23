package eu.stratosphere.pact4s.common

import eu.stratosphere.pact.common.`type`._
import eu.stratosphere.pact.common.`type`.base._

trait PactSerializerFactory[T] {

  val fieldCount: Int

  def createInstance(indexMap: Array[Int]): PactSerializer

  abstract class PactSerializer {
    def serialize(item: T, record: PactRecord)
    def deserialize(record: PactRecord): T
  }
}