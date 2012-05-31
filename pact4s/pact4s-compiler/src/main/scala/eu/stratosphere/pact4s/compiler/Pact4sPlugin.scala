package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.Plugin
import scala.tools.nsc.plugins.PluginComponent

import eu.stratosphere.pact4s.compiler.util.Logger

class Pact4sPlugin(val global: Global) extends Plugin with Pact4sGlobal {

  import global._

  val thisGlobal: ThisGlobal = global

  object udtAnalyzer extends UDTAnalyzer {
    override val global: ThisGlobal = thisGlobal
    override val runsAfter = List[String]("refchecks");
    override val runsRightAfter = Some("refchecks")
  }

  object udtCodeGen extends UDTCodeGenerator {
    override val global: ThisGlobal = thisGlobal
    override val runsAfter = List[String]("Pact4s.UDTAnalyzer");
    override val runsRightAfter = Some("Pact4s.UDTAnalyzer")
  }

  override val name = "Pact4s"
  override val description = "Performs analysis and code generation for Pact4s programs."
  override val components = List[PluginComponent](udtAnalyzer, udtCodeGen)
}

trait Pact4sGlobal extends UDTDescriptors with UDTAnalysis with UDTMetaDataAnalysis with UDTCodeGen with Logger {

  val global: Global
  type ThisGlobal = Pact4sGlobal.this.global.type

  import global._

  def getUDT: Type => UDTDescriptor
  val getGenSites: CompilationUnit => Tree => mutable.Set[UDTDescriptor]
}
