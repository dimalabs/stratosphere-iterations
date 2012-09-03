package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.SubComponent

trait Visualizers {

  val global: Global
  import global._

  trait Visualize extends Transform {

    var visualize: Boolean = false

    override def afterRun() = {
      super.afterRun()
      if (visualize) treeBrowser.browse(phaseName, currentRun.units)
    }
  }
}