package eu.stratosphere.pact4s.compiler.udf

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait Unlifters { this: Pact4sPlugin =>

  import global._
  import defs._

  trait Unlifter { this: TypingTransformer with TreeGenerator with Logger =>

    /*
       * Convert: unanalyzedFieldSelectorCode(fun) => unanalyzedFieldSelector(fun)
       *          unanalyzedUDF1Code(fun)          => unanalyzedUDF1(fun)
       *          unanalyzedUDF2Code(fun)          => unanalyzedUDF2(fun)
       */
    protected def unlift(tree: Tree): Tree = tree match {

      case LiftedView(unliftedView, tparams, fun) => {

        // ref is unanalyzedFieldSelector, unanalyzedUDF1, unanalyzedUDF2, or a user-supplied view
        val ref = inferImplicitView(mkViewTpe(unliftedView, tparams), dontInfer = Set()).get
        localTyper.typed { Apply(ref, List(fun)) }
      }

      case _ => tree

    }

    private object LiftedView {

      def unapply(tree: Tree): Option[(Symbol, List[Type], Function)] = tree match {

        /*
           * Extract: unanalyzedFieldSelectorCode[A, B](Code.lift(fun: A => B)) => (unanalyzedFieldSelector, List(A, B), fun)
           *          unanalyzedUDF1Code[A, B](Code.lift(fun: A => B))          => (unanalyzedUDF1, List(A, B), fun)
           *          unanalyzedUDF2Code[A, B, C](Code.lift(fun: (A, B) => C))  => (unanalyzedUDF2, List(A, B, C), fun)
           */
        case Apply(TypeApply(Unlifted(kind), tparams), List(CodeLift(fun: Function))) => Some((kind, tparams map { _.tpe }, fun))
        case _ => None
      }

      private object CodeLift {

        // Extract: scala.reflect.Code.lift[T](fun: T) => fun
        def unapply(tree: Tree): Option[Function] = tree match {
          case Apply(lift, List(fun: Function)) if lift.symbol == liftMethod => Some(fun)
          case _ => None
        }
      }
    }

    /*
       * Get the type of view needed to convert a FunctionN to the requested kind of UDF
       * 
       * mkViewTpe(unanalyzedFieldSelector, List(A, B)) = (A => B)      => FieldSelectorCode[A => B]
       * mkViewTpe(unanalyzedUDF1, List(A, B))          = (A => B)      => UDF1Code[A => B]
       * mkViewTpe(unanalyzedUDF2, List(A, B, C))       = ((A, B) => C) => UDF2Code[(A, B) => C]
       */
    private def mkViewTpe(kind: Symbol, tparams: List[Type]): Type = (kind, tparams) match {
      case (`unanalyzedFieldSelector`, List(t1, r)) => mkFunctionType(mkFunctionType(t1, r), mkFieldSelectorCodeOf(t1, r))
      case (`unanalyzedUDF1`, List(t1, r))          => mkFunctionType(mkFunctionType(t1, r), mkUDF1CodeOf(t1, r))
      case (`unanalyzedUDF2`, List(t1, t2, r))      => mkFunctionType(mkFunctionType(t1, t2, r), mkUDF2CodeOf(t1, t2, r))
      case _                                        => NoType
    }
  }
}