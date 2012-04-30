package eu.stratosphere.pact4s.common.streams

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.stubs._
import eu.stratosphere.pact4s.common.stubs.parameters._

import eu.stratosphere.pact.common.contract._

case class ReduceStream[Key, In: UDT, Out: UDT, GroupByKeySelector: SelectorBuilder[In, Key]#Selector, FC: UDF1Builder[Iterator[In], In]#UDF, FR: UDF1Builder[Iterator[In], Out]#UDF](
  input: DataStream[In],
  keySelector: In => Key,
  combineFunction: Option[Iterator[In] => In],
  reduceFunction: Iterator[In] => Out)
  extends DataStream[Out] {

  override def contract = {

    val stub = combineFunction map { _ => classOf[CombinableReduce4sStub[In, Out]] } getOrElse classOf[Reduce4sStub[In, Out]]
    val inputUDT = implicitly[UDT[In]]
    val outputUDT = implicitly[UDT[Out]]
    val combinerUDF = combineFunction map { _ => implicitly[UDF1[Iterator[In] => In]] }
    val reducerUDF = implicitly[UDF1[Iterator[In] => Out]]
    val name = getPactName getOrElse "<Unnamed Reducer>"

    val keyFieldSelector = implicitly[FieldSelector[In => Key]]
    val keyFieldTypes = inputUDT.getKeySet(keyFieldSelector.getFields)

    new ReduceContract(stub, keyFieldTypes, keyFieldSelector.getFields, input.getContract, name) with Reduce4sContract[Key, In, Out] {

      override val keySelector = keyFieldSelector
      override val stubParameters = new ReduceParameters(inputUDT, outputUDT, combinerUDF, combineFunction, reducerUDF, reduceFunction)
    }
  }
}

