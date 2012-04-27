package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.`type`.{ Key => PactKey }

case class ReduceStream[Key, In: UDT, Out: UDT, GroupByKeySelector: KeyBuilder[In, Key]#Selector, FC: UDF1Builder[Iterator[In], In]#UDF, FR: UDF1Builder[Iterator[In], Out]#UDF](
  input: DataStream[In],
  keySelector: In => Key,
  combineFunction: Option[Iterator[In] => In],
  reduceFunction: Iterator[In] => Out)
  extends DataStream[Out] {

  override def contract = {

    val stub = combineFunction map { _ => classOf[CombinableReduce4sStub[Key, In, Out]] } getOrElse classOf[Reduce4sStub[Key, In, Out]]
    val inputUDT = implicitly[UDT[In]]
    val outputUDT = implicitly[UDT[Out]]
    val keySelector = implicitly[KeySelector[In => Key]]
    val combinerUDF = combineFunction map { _ => implicitly[UDF1[Iterator[In] => In]] }
    val reducerUDF = implicitly[UDF1[Iterator[In] => Out]]
    val name = getPactName getOrElse "<Unnamed Reducer>"

    val keyTypes = new Array[Class[_ <: PactKey]](0)

    new ReduceContract(stub, keyTypes, keySelector.readFields, input.getContract, name) with ParameterizedContract[ReduceParameters[Key, In, Out]] {

      override val stubParameters = new ReduceParameters(inputUDT, outputUDT, keySelector, combinerUDF, combineFunction, reducerUDF, reduceFunction)
    }
  }
}
