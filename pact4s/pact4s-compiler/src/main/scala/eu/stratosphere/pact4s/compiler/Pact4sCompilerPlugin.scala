package eu.stratosphere.pact4s.compiler

import scala.tools.nsc
import nsc.Global
import nsc.Phase
import nsc.plugins.Plugin
import nsc.plugins.PluginComponent

class Pact4sCompilerPlugin(val global: Global) extends Plugin {
  import global._

  override val name = "Pact4s"
  override val description = "Performs analysis and code generation for Pact4s programs."

  object udtGenerator extends UDTGenerator {
    override val global: Pact4sCompilerPlugin.this.global.type = Pact4sCompilerPlugin.this.global
    override val runsAfter = List[String]("refchecks");
  }

  override val components = List[PluginComponent](udtGenerator)
}