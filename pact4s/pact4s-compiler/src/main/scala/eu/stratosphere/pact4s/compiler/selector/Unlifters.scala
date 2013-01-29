/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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