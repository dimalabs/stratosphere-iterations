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
import eu.stratosphere.pact.compiler.plan.OptimizedPlan
import eu.stratosphere.pact.compiler.postpass.OptimizerPostPass

abstract class PactDescriptor[T <: PactProgram: Manifest] extends PlanAssembler
  with PlanAssemblerDescription with OptimizerPostPass
  with GlobalSchemaGenerator with GlobalSchemaOptimizer {

  val name: String = implicitly[Manifest[T]].toString
  val parameters: String

  override def getDescription = "Parameters: [-subtasks <int:1>] [-nohints] [-nocompact] " + parameters

  override def getPlan(rawargs: String*): Plan = {

    val args = Pact4sArgs.parse(rawargs)
    val program = createInstance(args)

    val sinks = Pact4sContractFactory.withEnvironment {
      InputHintable.withEnabled(args.schemaHints) {
        program.outputs map { _.getContract.asInstanceOf[DataSink4sContract[_]] }
      }
    }

    initGlobalSchema(sinks)

    val plan = new Plan(sinks, name)
    plan.setDefaultParallelism(args.defaultParallelism)
    plan.getPlanConfiguration().setBoolean("Pact4s::SchemaCompaction", args.schemaCompaction)
    plan
  }

  override def postPass(plan: OptimizedPlan): Unit = {
    optimizeSchema(plan, plan.getPlanConfiguration().getBoolean("Pact4s::SchemaCompaction", true))
  }

  def createInstance(args: Pact4sArgs): T = {

    val clazz = implicitly[Manifest[T]].erasure

    try {

      val constr = optionally { clazz.getConstructor(classOf[Pact4sArgs]).asInstanceOf[Constructor[Any]] }
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

case class Pact4sArgs(argsMap: Map[String, String], defaultParallelism: Int, schemaHints: Boolean, schemaCompaction: Boolean) {
  def apply(key: String): String = argsMap.getOrElse(key, key)
  def apply(key: String, default: => String) = argsMap.getOrElse(key, default)
}

object Pact4sArgs {

  def parse(args: Seq[String]): Pact4sArgs = {

    var argsMap = Map[String, String]()
    var defaultParallelism = 1
    var schemaHints = true
    var schemaCompaction = true

    val ParamName = "-(.+)".r

    def parse(args: Seq[String]): Unit = args match {
      case Seq("-subtasks", value, rest @ _*)     => { defaultParallelism = value.toInt; parse(rest) }
      case Seq("-nohints", rest @ _*)             => { schemaHints = false; parse(rest) }
      case Seq("-nocompact", rest @ _*)           => { schemaCompaction = false; parse(rest) }
      case Seq(ParamName(name), value, rest @ _*) => { argsMap = argsMap.updated(name, value); parse(rest) }
      case Seq()                                  =>
    }

    parse(args)
    Pact4sArgs(argsMap, defaultParallelism, schemaHints, schemaCompaction)
  }
}

class Pact4sInstantiationException(cause: Throwable) extends Exception("Could not instantiate program.", cause)

