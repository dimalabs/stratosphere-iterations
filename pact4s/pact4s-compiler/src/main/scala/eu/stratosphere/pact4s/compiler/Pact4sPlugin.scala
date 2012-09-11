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
import scala.tools.nsc.SubComponent
import scala.tools.nsc.plugins.Plugin
import scala.tools.nsc.plugins.PluginComponent

import eu.stratosphere.pact4s.compiler.udt._
import eu.stratosphere.pact4s.compiler.udf._
import eu.stratosphere.pact4s.compiler.util._

class Pact4sPlugin(val global: Global) extends Plugin with Pact4sPluginOptions with HasGlobal
  with TypingTransformers with TreeGenerators with Loggers with Visualizers
  with Definitions with UDTDescriptors with UDTAnalyzers
  with UDTGenSiteParticipants with UDTGenSiteSelectors with UDTGenSiteTransformers
  with UDFAnalyzers with SanityCheckers {

  import global._

  override val name = "pact4s"
  override val description = "Performs analysis and code generation for Pact4s programs."
  override val components = List[PluginComponent](udtSite, udtCode, udfAna, sanity)
  override val optionsHelp = getOptionsHelp
  override val neverInfer = defs.unanalyzed

  object udtSite extends Pact4sPhase("UDTSite", refchecks) with UDTGenSiteSelector
  object udtCode extends Pact4sPhase("UDTCode", udtSite) with UDTGenSiteTransformer
  object udfAna extends Pact4sPhase("UDFAna", udtCode) with UDFAnalyzer
  object sanity extends Pact4sPhase("Sanity", udfAna) with SanityChecker

  abstract class Pact4sComponent extends PluginComponent with Transform with Visualize {
    override val global: Pact4sPlugin.this.global.type = Pact4sPlugin.this.global
  }

  abstract class Pact4sPhase(name: String, runAfter: SubComponent) extends Pact4sComponent {
    override def toString = name
    override val phaseName = "%s:%s".format(Pact4sPlugin.this.name, name)
    override val runsAfter = List[String](runAfter.phaseName)
    override val runsBefore = List[String](liftcode.phaseName)
  }
}

trait Pact4sPluginOptions { this: Pact4sPlugin =>

  protected def getOptionsHelp: Option[String] = {

    val items = Seq(
      ("verbosity:<value>", "Set the output verbosity to <value>. (error, warn, debug, inspect) default:warn"),
      ("inspect:<phase>", "Show trees after <phase>. (none, " + components.mkString(", ") + ") default:none")
    )

    val optName = "  -P:" + name + ":%s"
    val optLine = "%-30s%s"
    val options = items map { case (opt, desc) => optLine.format(optName.format(opt), desc) } mkString ("\n")

    Some(options)
  }

  override def processOptions(options: List[String], error: String => Unit): Unit = {

    val VerbosityPattern = "verbosity:(.+)".r
    val InspectPattern = "inspect:(.+)".r
    object Component { def unapply(name: String): Option[PluginComponent] = components.find(_.toString.toLowerCase == name.toLowerCase) }

    for (opt <- options) {
      opt match {
        case VerbosityPattern(LogLevel(level)) => logger.level = level
        case InspectPattern(Component(phase: Visualize)) => phase.visualize = true
        case _ => error("Unrecognized option -P:%s:%s".format(name, opt))
      }
    }
  }
}

