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

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.Plugin
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

import eu.stratosphere.pact4s.compiler.util._

class Pact4sPlugin(val global: Global) extends Plugin
  with TypingTransformers with TypingTraversers
  with UDTDescriptors with UDTAnalyzers
  with UDTGenSiteParticipants with UDTGenSiteSelectors with UDTGenSiteTransformers
  with TreeGenerators with Definitions with Loggers {

  import global._

  abstract class Pact4sTransform extends Pact4sPluginComponent with Transform
  abstract class Pact4sPluginComponent extends PluginComponent with Logger {
    val global: Pact4sPlugin.this.global.type = Pact4sPlugin.this.global
  }

  object udtGenSiteSelector extends UDTGenSiteSelector {
    override val phaseName = "Pact4s.UDTSite"
    override val runsAfter = List[String]("refchecks");
    override val runsRightAfter = Some("refchecks")
  }

  object udtGenSiteTransformer extends UDTGenSiteTransformer {
    override val phaseName = "Pact4s.UDTCode"
    override val runsAfter = List[String]("Pact4s.UDTSite");
    override val runsRightAfter = Some("Pact4s.UDTSite")
  }

  override val name = "Pact4s"
  override val description = "Performs analysis and code generation for Pact4s programs."
  override val components = List[PluginComponent](udtGenSiteSelector, udtGenSiteTransformer)
}

