/**
 * *********************************************************************************************************************
 *
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
 *
 * ********************************************************************************************************************
 */

package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.Plugin
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.TypingTransformers

import eu.stratosphere.pact4s.compiler.util._

class Pact4sPlugin(val global: Global) extends Plugin with Pact4sGlobal {

  import global._

  val thisGlobal: ThisGlobal = global

  val thisGenSites = {
    val initial = mutable.Map[CompilationUnit, MutableMultiMap[Tree, UDTDescriptor]]()
    initial withDefault { unit =>
      val unitGenSites = new MutableMultiMap[Tree, UDTDescriptor]()
      initial(unit) = unitGenSites
      unitGenSites
    }
  }

  object udtGenSiteSelector extends UDTGenSiteSelector {
    override val global: ThisGlobal = thisGlobal
    override val genSites = thisGenSites
    override val runsAfter = List[String]("refchecks");
    override val runsRightAfter = Some("refchecks")
  }

  object udtCodeGen extends UDTCodeGenerator {
    override val global: ThisGlobal = thisGlobal
    override val genSites = thisGenSites
    override val runsAfter = List[String]("Pact4s.UDTGenSiteSelection");
    override val runsRightAfter = Some("Pact4s.UDTGenSiteSelection")
  }

  override val name = "Pact4s"
  override val description = "Performs analysis and code generation for Pact4s programs."
  override val components = List[PluginComponent](udtGenSiteSelector, udtCodeGen)
}

trait Pact4sGlobal extends Object
  with TypingTransformers
  with Traversers
  with UDTDescriptors
  with UDTAnalysis
  with UDTGenSiteSelection
  with UDTCodeGeneration
  with TreeGen
  with Logger {

  val global: Global
  type ThisGlobal = Pact4sGlobal.this.global.type
}

