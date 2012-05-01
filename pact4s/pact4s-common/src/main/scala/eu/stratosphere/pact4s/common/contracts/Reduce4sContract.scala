package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Reduce4sContract[Key, In, Out] extends Pact4sContract { this: ReduceContract =>

  val keySelector: FieldSelector[In => Key]
  val inputUDT: UDT[In]
  val outputUDT: UDT[Out]
  val combineUDF: UDF1[Iterator[In] => In]
  val reduceUDF: UDF1[Iterator[In] => Out]
  val userCombineFunction: Option[Iterator[In] => In]
  val userReduceFunction: Iterator[In] => Out

  override def persistConfiguration() = {

    val combineDeserializer = userCombineFunction map { _ => inputUDT.createSerializer(combineUDF.getReadFields) }
    val combineSerializer = userCombineFunction map { _ => inputUDT.createSerializer(combineUDF.getWriteFields) }
    val combineForward = userCombineFunction map { _ => combineUDF.getForwardedFields.toArray }

    val reduceDeserializer = inputUDT.createSerializer(reduceUDF.getReadFields)
    val reduceSerializer = outputUDT.createSerializer(reduceUDF.getWriteFields)
    val reduceForward = reduceUDF.getForwardedFields.toArray

    val stubParameters = new ReduceParameters(combineDeserializer, combineSerializer, combineForward, userCombineFunction, reduceDeserializer, reduceSerializer, reduceForward, userReduceFunction)
    stubParameters.persist(this)
  }
}

object Reduce4sContract {
  def unapply(c: Reduce4sContract[_, _, _]) = Some((c.keySelector, c.inputUDT, c.outputUDT, c.combineUDF, c.reduceUDF))
}