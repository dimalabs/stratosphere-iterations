package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Phase
import scala.tools.nsc.plugins.PluginComponent

trait Transform extends PluginComponent {

  import global._

  protected def newTransformer(unit: global.CompilationUnit): global.Transformer

  protected def afterRun() = {}

  def newPhase(prev: Phase): StdPhase = new StdPhase(prev) {

    override def run() = {
      super.run()
      afterRun()
    }

    def apply(unit: global.CompilationUnit) {
      newTransformer(unit).transformUnit(unit)
    }
  }
}
