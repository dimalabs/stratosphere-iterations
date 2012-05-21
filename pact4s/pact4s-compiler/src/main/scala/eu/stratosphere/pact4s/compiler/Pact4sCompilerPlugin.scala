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
  override val components = List[PluginComponent](Component)

  private object Component extends PluginComponent {

    override val global: Pact4sCompilerPlugin.this.global.type = Pact4sCompilerPlugin.this.global
    override val runsAfter = List[String]("refchecks");
    override val phaseName = Pact4sCompilerPlugin.this.name

    override def newPhase(prev: Phase) = new TestPhase(prev)

    class TestPhase(prev: Phase) extends StdPhase(prev) {

      override def name = Pact4sCompilerPlugin.this.name

      override def apply(unit: CompilationUnit) {
        unit.warning(unit.position(0), "This is a test")
      }
    }
  }
}