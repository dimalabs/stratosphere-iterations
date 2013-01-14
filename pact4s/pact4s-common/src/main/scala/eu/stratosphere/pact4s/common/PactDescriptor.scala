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

package eu.stratosphere.pact4s.common

import java.lang.reflect.Constructor

import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.analysis.postPass._

import eu.stratosphere.pact.common.plan._

abstract class PactDescriptor[T <: PactProgram: Manifest] extends PlanAssembler with PlanAssemblerDescription
  with GlobalSchemaGenerator with GlobalSchemaOptimizer {

  val name: String = implicitly[Manifest[T]].toString
  val description: String

  def getName(args: Map[Int, String]) = name
  def getDefaultParallelism(args: Map[Int, String]) = 1
  override def getDescription = description

  override def getPlan(args: String*): Plan = {

    val argsMap = args.zipWithIndex.map (_.swap).toMap
    val program = createInstance(argsMap)

    val sinks = Pact4sContractFactory.withEnvironment {
      program.outputs map { _.getContract.asInstanceOf[DataSink4sContract[_]] }
    }

    initGlobalSchema(sinks)

    val plan = new Plan(sinks, getName(argsMap))
    plan.setDefaultParallelism(getDefaultParallelism(argsMap))
    plan
  }

  def createInstance(args: Map[Int, String]): T = {

    val clazz = implicitly[Manifest[T]].erasure

    try {

      val constr = optionally { clazz.getConstructor(classOf[Map[Int, String]]).asInstanceOf[Constructor[Any]] }
      val inst = constr map { _.newInstance(args) } getOrElse { clazz.newInstance }

      inst.asInstanceOf[T]

    } catch {
      case e => throw new Pact4sInstantiationException(e)
    }
  }

  private def optionally[S](block: => S): Option[S] = {
    try {
      Some(block)
    } catch {
      case _ => None
    }
  }
}

class Pact4sInstantiationException(cause: Throwable) extends Exception("Could not instantiate program.", cause)

