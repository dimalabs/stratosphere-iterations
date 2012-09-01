package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global
import scala.tools.nsc.symtab.Flags
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

trait ScopingTransformers { this: TypingTransformers =>

  import global._

  trait ScopingTransformer { this: TypingTransformer =>

    private def treesToSyms(trees: Seq[Tree]): Seq[Symbol] = trees filter { _.hasSymbol } map { _.symbol }

    def withImplicits[T](stat: Tree)(trans: => T): T = stat match {
      case Block(stats, ret)             => withImplicits(treesToSyms(stats :+ ret)) { trans }
      case Function(vparams, _)          => withImplicits(treesToSyms(vparams)) { trans }
      case DefDef(_, _, _, params, _, _) => withImplicits(treesToSyms(params.flatten)) { trans }
      case _                             => trans
    }

    def withImplicits[T](syms: Seq[Symbol])(trans: => T): T = syms filter { sym => sym.isImplicit } match {

      case Seq() => trans

      case imps => {

        val savedScope = localTyper.context.scope

        localTyper.context.scope = savedScope.cloneScope
        imps foreach { localTyper.context.scope.enter }
        localTyper.context.resetCache()

        val ret = trans

        localTyper.context.scope = savedScope
        localTyper.context.resetCache()

        ret
      }
    }
  }
}