package eu.stratosphere.pact4s.common.analysis

import scala.collection.mutable

import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact.common.contract._

class GlobalSchemaPrinter {

  private val printed = mutable.Set[Contract]()

  def printSchema(contract: Contract, proxies: Map[Contract, Pact4sContract[_]]) = {

    if (printed.add(contract)) {

      contract match {

        case c @ DataSink4sContract(input) => {
          println(c.getName() + " (Sink): [" + c.udf.inputFields.toSerializerIndexArray.mkString(", ") + "] -> Format")
        }

        case c: DataSource4sContract[_] => {
          println(c.getName() + " (Source): Parse -> [" + c.udf.outputFields.toSerializerIndexArray.mkString(", ") + "]")
        }

        case c @ Iterate4sContract(s0, step, term, placeholder) => {
          println(contract.getName() + " (Iterate)")
        }

        case c @ WorksetIterate4sContract(s0, ws0, deltaS, newWS, placeholderS, placeholderWS) => {
          val keyFields = c.key.selectedFields.toSerializerIndexArray
          println(contract.getName() + " (WorksetIterate) {" + keyFields.mkString(", ") + "}")
        }

        case c @ CoGroup4sContract(left, right) => {
          val keyFields = c.leftKey.selectedFields.toSerializerIndexArray.map("L" + _) ++ c.rightKey.selectedFields.toSerializerIndexArray.map("R" + _)
          val readFields = c.udf.leftInputFields.toSerializerIndexArray.map("L" + _) ++ c.udf.rightInputFields.toSerializerIndexArray.map("R" + _)
          val forwardedFields = (c.udf.getLeftForwardIndexArray.map("L" + _) ++ c.udf.getRightForwardIndexArray.map("R" + _)).sorted
          val writeFields = c.udf.outputFields.toSerializerIndexArray.filter(_ >= 0)
          println(c.getName() + " (CoGroup) {" + keyFields.mkString(", ") + "}: [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
        }

        case c @ Cross4sContract(left, right) => {
          val readFields = c.udf.leftInputFields.toSerializerIndexArray.map("L" + _) ++ c.udf.rightInputFields.toSerializerIndexArray.map("R" + _)
          val forwardedFields = (c.udf.getLeftForwardIndexArray.map("L" + _) ++ c.udf.getRightForwardIndexArray.map("R" + _)).sorted
          val writeFields = c.udf.outputFields.toSerializerIndexArray.filter(_ >= 0)
          println(c.getName() + " (Cross): [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
        }

        case c @ Join4sContract(left, right) => {
          val keyFields = c.leftKey.selectedFields.toSerializerIndexArray.map("L" + _) ++ c.rightKey.selectedFields.toSerializerIndexArray.map("R" + _)
          val readFields = c.udf.leftInputFields.toSerializerIndexArray.map("L" + _) ++ c.udf.rightInputFields.toSerializerIndexArray.map("R" + _)
          val forwardedFields = (c.udf.getLeftForwardIndexArray.map("L" + _) ++ c.udf.getRightForwardIndexArray.map("R" + _)).sorted
          val writeFields = c.udf.outputFields.toSerializerIndexArray.filter(_ >= 0)
          println(c.getName() + " (Join) {" + keyFields.mkString(", ") + "}: [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
        }

        case c @ Map4sContract(input) => {
          val readFields = c.udf.inputFields.toSerializerIndexArray
          val forwardedFields = c.udf.getForwardIndexArray.sorted
          val writeFields = c.udf.outputFields.toSerializerIndexArray.filter(_ >= 0)
          println(c.getName() + " (Map): [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
        }

        case c @ Reduce4sContract(input) => {
          val keyFields = c.key.selectedFields.toSerializerIndexArray
          val cReadFields = c.combineUDF.inputFields.toSerializerIndexArray
          val cWriteFields = c.combineUDF.outputFields.toSerializerIndexArray.filter(_ >= 0)
          val rReadFields = c.udf.inputFields.toSerializerIndexArray
          val rWriteFields = c.udf.outputFields.toSerializerIndexArray.filter(_ >= 0)
          val forwardedFields = c.udf.getForwardIndexArray.sorted
          println(c.getName() + " (Combine) {" + keyFields.mkString(", ") + "}: [" + cReadFields.mkString(", ") + "] -> [" + cWriteFields.mkString(", ") + "]")
          println((" " * c.getName().length) + " (Reduce) {" + keyFields.mkString(", ") + "}: [" + rReadFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + rWriteFields.mkString(", ") + "]")
        }
 
        case proxy => {
          println(proxies(proxy).getName() + " (Proxy)")
        }
      }
    }
  }
}