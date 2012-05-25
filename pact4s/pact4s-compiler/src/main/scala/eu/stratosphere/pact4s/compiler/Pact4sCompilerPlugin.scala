package eu.stratosphere.pact4s.compiler

import scala.collection.mutable
import scala.tools.nsc.Global
import scala.tools.nsc.plugins.Plugin
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.symtab.SymbolTable
import scala.tools.nsc.symtab.Symbols
import scala.tools.nsc.symtab.Types

class Pact4sCompilerPlugin(val global: Global) extends Plugin {
  import global._

  object udtDescriptors extends UDTDescriptors {
    override val global = Pact4sCompilerPlugin.this.global
  }
  
  object udtAnalyzer extends UDTAnalyzer(udtDescriptors) {
    override val runsAfter = List[String]("refchecks");
    override val runsRightAfter = Some("refchecks")
  }

  object udtGenerator extends UDTGenerator(udtDescriptors) {
    override val runsAfter = List[String]("Pact4s.UDTAnalyzer");
    override val runsRightAfter = Some("Pact4s.UDTAnalyzer")
  }

  override val name = "Pact4s"
  override val description = "Performs analysis and code generation for Pact4s programs."
  override val components = List[PluginComponent](udtAnalyzer, udtGenerator)
}

