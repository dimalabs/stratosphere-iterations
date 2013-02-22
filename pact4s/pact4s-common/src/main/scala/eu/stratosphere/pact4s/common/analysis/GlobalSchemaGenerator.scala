/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.pact4s.common.analysis

import scala.collection.JavaConversions._
import scala.util.DynamicVariable
import java.util.{ List => JList }
import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.generic.contract._
import eu.stratosphere.pact.compiler.plan.OptimizerNode

trait GlobalSchemaGenerator {

  def initGlobalSchema(sinks: Seq[DataSink4sContract[_]]): Unit = {

    sinks.foldLeft(0) { (freePos, contract) => globalizeContract(contract, Seq(), Map(), None, freePos) }
  }

  /**
   * Computes disjoint write sets for a contract and its inputs.
   *
   * @param contract The contract to globalize
   * @param parentInputs Input fields which should be bound to the contract's outputs
   * @param proxies Provides contracts for iteration placeholders
   * @param fixedOutputs Specifies required positions for the contract's output fields, or None to allocate new positions
   * @param freePos The current first available position in the global schema
   * @return The new first available position in the global schema
   */
  private def globalizeContract(contract: Contract, parentInputs: Seq[FieldSet[InputField]], proxies: Map[Contract, Pact4sContract[_]], fixedOutputs: Option[FieldSet[Field]], freePos: Int): Int = {

    val contract4s = proxies.getOrElse(contract, contract.asInstanceOf[Pact4sContract[_]])

    parentInputs.foreach(contract4s.getUDF.attachOutputsToInputs)

    contract4s.getUDF.outputFields.isGlobalized match {

      case true => freePos

      case false => {

        val freePos1 = globalizeContract(contract4s, proxies, fixedOutputs, freePos)

        eliminateNoOps(contract4s)
        contract4s.persistConfiguration(None)

        freePos1
      }
    }
  }

  private def globalizeContract(contract: Pact4sContract[_], proxies: Map[Contract, Pact4sContract[_]], fixedOutputs: Option[FieldSet[Field]], freePos: Int): Int = {

    contract match {

      case contract @ DataSink4sContract(input) => {
        contract.udf.outputFields.setGlobalized()
        globalizeContract(input, Seq(contract.udf.inputFields), proxies, None, freePos)
      }

      case contract: DataSource4sContract[_] => {
        contract.udf.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract @ Iterate4sContract(s0, step, term, placeholder) => {

        val s0contract = proxies.getOrElse(s0, s0.asInstanceOf[Pact4sContract[_]])
        val newProxies = proxies + (placeholder -> s0contract)

        val freePos1 = globalizeContract(s0, Seq(), proxies, fixedOutputs, freePos)
        val freePos2 = globalizeContract(step, Seq(), newProxies, Some(s0contract.getUDF.outputFields), freePos1)
        val freePos3 = term map { globalizeContract(_, Seq(), newProxies, None, freePos2) } getOrElse freePos2

        contract.udf.assignOutputGlobalIndexes(s0contract.getUDF.outputFields)

        freePos3
      }

      case contract @ WorksetIterate4sContract(s0, ws0, deltaS, newWS, placeholderS, placeholderWS) => {

        val s0contract = proxies.getOrElse(s0.get(0), s0.asInstanceOf[Pact4sContract[_]])
        val ws0contract = proxies.getOrElse(ws0.get(0), ws0.asInstanceOf[Pact4sContract[_]])
        val newProxies = proxies + (placeholderS -> s0contract) + (placeholderWS -> ws0contract)

        val freePos1 = globalizeContract(s0.get(0), Seq(contract.key.inputFields), proxies, fixedOutputs, freePos)
        val freePos2 = globalizeContract(ws0.get(0), Seq(), proxies, None, freePos1)
        val freePos3 = globalizeContract(deltaS, Seq(), newProxies, Some(s0contract.getUDF.outputFields), freePos2)
        val freePos4 = globalizeContract(newWS, Seq(), newProxies, Some(ws0contract.getUDF.outputFields), freePos3)

        contract.udf.assignOutputGlobalIndexes(s0contract.getUDF.outputFields)

        freePos4
      }

      case contract @ CoGroup4sContract(leftInput, rightInput) => {

        val freePos1 = globalizeContract(leftInput, Seq(contract.udf.leftInputFields, contract.leftKey.inputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(rightInput, Seq(contract.udf.rightInputFields, contract.rightKey.inputFields), proxies, None, freePos1)

        contract.udf.setOutputGlobalIndexes(freePos2, fixedOutputs)
      }

      case contract @ Cross4sContract(leftInput, rightInput) => {

        val freePos1 = globalizeContract(leftInput, Seq(contract.udf.leftInputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(rightInput, Seq(contract.udf.rightInputFields), proxies, None, freePos1)

        contract.udf.setOutputGlobalIndexes(freePos2, fixedOutputs)
      }

      case contract @ Join4sContract(leftInput, rightInput) => {

        val freePos1 = globalizeContract(leftInput, Seq(contract.udf.leftInputFields, contract.leftKey.inputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(rightInput, Seq(contract.udf.rightInputFields, contract.rightKey.inputFields), proxies, None, freePos1)

        contract.udf.setOutputGlobalIndexes(freePos2, fixedOutputs)
      }

      case contract @ Map4sContract(input) => {

        val freePos1 = globalizeContract(input, Seq(contract.udf.inputFields), proxies, None, freePos)

        contract.udf.setOutputGlobalIndexes(freePos1, fixedOutputs)
      }

      case contract @ Reduce4sContract(input) => {

        val freePos1 = globalizeContract(input, Seq(contract.udf.inputFields, contract.key.inputFields), proxies, None, freePos)

        contract.udf.setOutputGlobalIndexes(freePos1, fixedOutputs)
      }

      case contract @ Union4sContract(inputs) => {

        // Determine where this contract's children should write their output 
        val freePos1 = contract.udf.setOutputGlobalIndexes(freePos, fixedOutputs)

        // If an input hasn't yet allocated its output fields, then we can force them into 
        // the expected position. Otherwise, the output fields must be physically copied.
        for (idx <- 0 until inputs.size()) {
          val input = inputs.get(idx)
          val input4s = proxies.getOrElse(input, input.asInstanceOf[Pact4sContract[_]])

          if (input4s.getUDF.outputFields.isGlobalized || input4s.getUDF.outputFields.exists(_.globalPos.isReference)) {
            inputs.set(idx, Copy4sContract(input4s))
          }
        }

        inputs.foldLeft(freePos1) { (freePos2, input) =>
          globalizeContract(input, Seq(), proxies, Some(contract.udf.outputFields), freePos2)
        }
      }

      case contract @ Copy4sContract(input) => {

        val freePos1 = globalizeContract(input, Seq(contract.udf.inputFields), proxies, None, freePos)

        contract.udf.setOutputGlobalIndexes(freePos1, fixedOutputs)
      }
    }
  }

  private def eliminateNoOps(contract: Contract): Unit = {

    def elim(children: JList[Contract]): Unit = {

      val newChildren = children flatMap {
        case NoOp4sContract(grandChildren) => grandChildren
        case child                         => List(child)
      }

      children.clear()
      children.addAll(newChildren)
    }

    contract match {
      case c: SingleInputContract[_] => elim(c.getInputs())
      case c: DualInputContract[_]   => elim(c.getFirstInputs()); elim(c.getSecondInputs())
      case _                         =>
    }
  }
}
