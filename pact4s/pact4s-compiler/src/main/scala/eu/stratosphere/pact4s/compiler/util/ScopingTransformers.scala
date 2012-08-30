package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

trait ScopingTransformers { this: TypingTransformers =>

  import global._

  trait ScopingTransformer { this: TypingTransformer =>

    def withImplicits[T](stat: Tree)(trans: => T): T = stat match {
      case Block(stats, ret)             => withImplicits(stats :+ ret) { trans }
      case Function(vparams, _)          => withImplicits(vparams) { trans }
      case DefDef(_, _, _, params, _, _) => withImplicits(params.flatten) { trans }
      case _                             => trans
    }

    def withImplicits[T](stats: Seq[Tree])(trans: => T): T = stats filter { stat => stat.hasSymbol && stat.symbol.isImplicit } map { _.symbol } match {

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