package eu.stratosphere.pact4s.compiler.util.treeReducers

import eu.stratosphere.pact4s.compiler.util._

trait NonReducibles { this: HasGlobal with Environments with Closures =>

  import global._

  case class NonReducible(reason: ReductionError, expr: Tree) extends TermTree {
    def panic: Unit = {
      var envs: List[Environment] = Nil
      val findEnvs = new Traverser {
        import ReductionError._
        override def traverse(tree: Tree) = tree match {
          case env: Environment                    => envs = env :: envs
          case Closure(env, _, body)               => envs = env :: envs; traverse(body)
          case NonReducible(CausedBy(cause), expr) => traverse(cause); traverse(expr)
          case NonReducible(_, expr)               => traverse(expr)
          case tree                                => super.traverse(tree)
        }
      }
      findEnvs.traverse(this)
      envs foreach { _.panic }
    }

    override def toString = "NonReducible(" + reason.toString + "): " + expr
  }

  object NonReducible {
    def panicked(reason: ReductionError, expr: Tree): NonReducible = {
      val ret = NonReducible(reason, expr)
      ret.panic
      ret
    }
  }

  sealed abstract class ReductionError
  object ReductionError {
    case object NotImplemented extends ReductionError
    case object Unexpected extends ReductionError
    case object Synthetic extends ReductionError
    case object NoSource extends ReductionError
    case object Recursive extends ReductionError
    case object NonDeterministic extends ReductionError

    class CausedBy(val cause: NonReducible) extends ReductionError {
      override def toString = "CausedBy(" + cause + ")"
    }

    object CausedBy {
      def unapply(err: CausedBy): Option[NonReducible] = Some(err.cause)
      def apply(inner: NonReducible): ReductionError = inner.reason match {
        case reason: CausedBy => apply(reason.cause)
        case _                => new CausedBy(inner)
      }
    }
  }
}