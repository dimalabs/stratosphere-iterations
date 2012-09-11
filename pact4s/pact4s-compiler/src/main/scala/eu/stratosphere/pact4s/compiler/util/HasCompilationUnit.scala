package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global

trait HasGlobal {
  
  val global: Global
  import global._
  
  trait HasCompilationUnit {
    protected def unit: CompilationUnit
  }
  
  trait HasPosition {
    protected def curPos: Position
  }
}