package eu.stratosphere.pact4s.compiler.util

import scala.collection.mutable
import scala.tools.nsc.transform.Transform

trait Traverse extends Transform {

  import global._

  protected def newTraverser(unit: CompilationUnit): Traverser
  override def newTransformer(unit: CompilationUnit) = newTraverser(unit)

  trait Traverser extends Transformer {

    private var path = Seq[Tree]()
    def getPath = path

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
}