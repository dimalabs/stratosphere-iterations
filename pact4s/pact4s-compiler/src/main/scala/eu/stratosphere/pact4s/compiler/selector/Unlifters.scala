package eu.stratosphere.pact4s.compiler.selector

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait Unlifters { this: Pact4sPlugin =>

  import global._
  import defs._

  trait Unlifter { this: TypingTransformer with TreeGenerator with Logger =>

    protected def unlift(tree: Tree): Tree = tree match {

      /*
       * Convert: unanalyzedFieldSelectorCode(Code.lift(fun)) => unanalyzedFieldSelector(fun)
       *          unanalyzedKeySelectorCode(Code.lift(fun))   => unanalyzedKeySelector(fun)
       */
      case Apply(TypeApply(CodeLifted(unlifted), tpes), List(Apply(codeLift, List(fun)))) if codeLift.symbol == codeLiftMethod => {

        val viewTpe = ((unlifted, tpes map { _.tpe }): @unchecked) match {
          case (`unanalyzedFieldSelector`, List(t1, r)) => mkFunctionType(mkFunctionType(t1, r), mkFieldSelectorOf(t1, r))
          case (`unanalyzedKeySelector`, List(t1, r))   => mkFunctionType(mkFunctionType(t1, r), mkKeySelectorOf(t1, r))
        }

        // ref is unanalyzedFieldSelector, unanalyzedKeySelector, or a user-supplied view
        val ref = inferImplicitView(viewTpe, dontInfer = Set())
        localTyper.typed { Apply(ref.get, List(fun)) }
      }

      case _ => tree
    }
  }
}