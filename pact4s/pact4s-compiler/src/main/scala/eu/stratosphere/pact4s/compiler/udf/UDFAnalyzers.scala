/**
 * *********************************************************************************************************************
 *
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
 *
 * ********************************************************************************************************************
 */

package eu.stratosphere.pact4s.compiler.udf

import eu.stratosphere.pact4s.compiler.Pact4sPlugin
import scala.tools.nsc.symtab.Flags

trait UDFAnalyzers extends SelectorAnalyzers with FlowAnalyzers with FallbackBinders { this: Pact4sPlugin =>

  import global._
  import defs._

  trait UDFAnalyzer extends Pact4sComponent {
    
    var snapshot: Map[Symbol, ValOrDefDef] = _
    
    override def beforeRun() = {

      def findValOrDefDef(sym: Symbol, trees: List[Tree]) = trees find {
        case ValDef(_, _, _, EmptyTree) => false
        case _: ValDef => true
        case _: DefDef => true
        case _ => false
      } map { tree => (sym, tree.asInstanceOf[ValOrDefDef]) }
      
      def collectValAndDefDefs(unit: CompilationUnit) = unit.body filter { t => t.hasSymbol && !t.symbol.hasFlag(Flags.MUTABLE) } groupBy (_.symbol) flatMap { case (k, vs) => findValOrDefDef(k, vs) }
            
      snapshot = currentRun.units.toSeq flatMap { collectValAndDefDefs(_) } toMap
    }

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with TreeGenerator with Logger with SymbolSnapshot with SelectorAnalyzer with FlowAnalyzer with FallbackBinder {

      val snapshot = UDFAnalyzer.this.snapshot
      
      override def apply(tree: Tree) = super.apply {
        unlift(tree) match {
          case FieldSelector(result) => localTyper.typed { result }
          case AnalyzedUDF(result)   => localTyper.typed { result }
          case DefaultUDF(result)    => localTyper.typed { result }
          case tree                  => tree
        }
      }

      protected def unlift(tree: Tree): Tree = tree match {

        /*
         * Convert: unanalyzedFieldSelectorCode(Code.lift(fun)) => unanalyzedFieldSelector(fun)
         *          unanalyzedUDF1Code(Code.lift(fun))          => unanalyzedUDF1(fun)
         *          unanalyzedUDF2Code(Code.lift(fun))          => unanalyzedUDF2(fun)
         */
        case Apply(TypeApply(Unlifted(kind), tpes), List(Apply(lift, List(fun)))) if lift.symbol == liftMethod => {

          val viewTpe = ((kind, tpes map { _.tpe }): @unchecked) match {
            case (`unanalyzedFieldSelector`, List(t1, r)) => mkFunctionType(mkFunctionType(t1, r), mkFieldSelectorCodeOf(t1, r))
            case (`unanalyzedUDF1`, List(t1, r))          => mkFunctionType(mkFunctionType(t1, r), mkUDF1CodeOf(t1, r))
            case (`unanalyzedUDF2`, List(t1, t2, r))      => mkFunctionType(mkFunctionType(t1, t2, r), mkUDF2CodeOf(t1, t2, r))
          }
          
          // ref is unanalyzedFieldSelector, unanalyzedUDF1, unanalyzedUDF2, or a user-supplied view
          val ref = inferImplicitView(viewTpe, dontInfer = Set())
          localTyper.typed { Apply(ref.get, List(fun)) }
        }

        case _ => tree
      }
    }
  }
}

