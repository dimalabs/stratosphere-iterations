package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.SubComponent

trait Visualizers { this: Loggers =>

  val global: Global
  import global._

  trait Visualize extends SubComponent {

    var visualize: Boolean = false

    abstract override def newPhase(prev: Phase) = {
      val inner = super.newPhase(prev)
      new StdPhase(prev) {
        override def apply(unit: global.CompilationUnit) = {}
        override def run = {
          inner.run
          if (visualize)
            treeBrowser.browse(phaseName, currentRun.units)
        }
      }
    }
  }
}