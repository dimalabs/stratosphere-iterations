package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global
import scala.tools.nsc.transform.{ TypingTransformers => NscTypingTransformers }

trait TypingTransformers {

  val global: Global
  import global._

  private[TypingTransformers] val nscTypingTransformers = new NscTypingTransformers {
    override val global: TypingTransformers.this.global.type = TypingTransformers.this.global
  }

  val neverInfer = Set[Symbol]()

  trait TypingVisitor {
    protected def unit: CompilationUnit
    protected def localTyper: analyzer.Typer
    protected var curTree: Tree

    private var envs: Seq[(Tree, Scope)] = Seq()

    protected def curPath: Seq[Tree] = envs.unzip._1
    protected def curPos: Position = curPath find { _.pos != NoPosition } map { _.pos } getOrElse NoPosition

    private def nonEmptyTree(tree: Tree): Option[Tree] = tree match {
      case EmptyTree                          => None
      case t if neverInfer.contains(t.symbol) => None
      case t                                  => Some(t)
    }

    // It doesn't matter what we pass here for the tree parameter - we're not interested in determining types based on implicit resolution
    protected def inferImplicitInst(tpe: Type): Option[Tree] = nonEmptyTree(analyzer.inferImplicit(EmptyTree, tpe, true, false, localTyper.context).tree)
    protected def inferImplicitView(tpe: Type): Option[Tree] = nonEmptyTree(analyzer.inferImplicit(EmptyTree, tpe, true, true, localTyper.context).tree)
    protected def inferImplicitView(from: Type, to: Type): Option[Tree] = inferImplicitView(definitions.functionType(List(from), to))

    protected def pre(tree: Tree) = {
      curTree = tree
      envs = (tree, localTyper.context.scope) +: envs
    }

    protected def post(tree: Tree) = {
      setScope(envs.head._2)
      envs = envs.tail
    }

    private def setScope(scope: Scope) = {
      if (localTyper.context.scope ne scope) {
        localTyper.context.scope = scope
        localTyper.context.resetCache()
      }
    }

    private[TypingTransformers] def enterLocalImplicits(tree: Tree): Unit = {

      val stats = tree match {
        case Block(stats, _)               => stats
        case Function(vparams, _)          => vparams
        case DefDef(_, _, _, params, _, _) => params.flatten
        case _                             => Nil
      }

      val syms = stats filter { stat => stat.hasSymbol && stat.symbol.isImplicit } map { _.symbol }

      syms match {
        case Nil => localTyper.context.scope
        case _ => {
          val scope = localTyper.context.scope.cloneScope
          syms foreach scope.enter
          setScope(scope)
        }
      }
    }
  }

  abstract class TypingTransformer(protected val unit: CompilationUnit) extends nscTypingTransformers.TypingTransformer(unit) with TypingVisitor {

    final override def transform(tree: Tree): Tree = {
      pre(tree)
      val ret = apply(tree)
      post(ret)
      ret
    }

    protected def apply(tree: Tree): Tree = {
      enterLocalImplicits(tree)
      super.transform(tree)
    }
  }

  abstract class TypingTraverser(protected val unit: CompilationUnit) extends nscTypingTransformers.TypingTransformer(unit) with TypingVisitor {

    final override def transform(tree: Tree): Tree = {
      pre(tree)
      apply(tree)
      post(tree)
      tree
    }

    protected def apply(tree: Tree): Unit = {
      enterLocalImplicits(tree)
      super.transform(tree)
    }
  }
}

