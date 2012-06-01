package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

trait Traversers { this: TypingTransformers =>

  val global: Global
  import global._

  trait Traverser extends Transformer {

    private var path = Seq[Tree]()
    def currentPath = path

    def traverse(tree: Tree): Unit = {
      super.transform(tree)
    }

    def isPathComponent(tree: Tree): Boolean = true

    override def transform(tree: Tree): Tree = {

      val isPathComp = isPathComponent(tree)

      if (isPathComp)
        path = tree +: path

      traverse(tree)

      if (isPathComp)
        path = path.tail

      tree
    }
  }

  class TypingTraverser(unit: CompilationUnit) extends TypingTransformer(unit) with Traverser
}