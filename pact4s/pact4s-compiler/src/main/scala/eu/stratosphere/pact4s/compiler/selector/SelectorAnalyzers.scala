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
import eu.stratosphere.pact4s.compiler.util._

trait SelectorAnalyzers extends Unlifters with SelectionExtractors { this: Pact4sPlugin =>

  import global._
  import defs._

  trait SelectorAnalyzer extends Pact4sComponent {

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with TreeGenerator with Logger with Unlifter with SelectionExtractor {

      override def apply(tree: Tree) = super.apply {

        unlift(tree) match {

          case FieldSelection(t1, r, fun, ret) => ret match {

            case Left(errs)             => errs foreach { err => Error.report("Error analyzing FieldSelector[" + mkFunctionType(t1, r) + "]: " + err) }; tree

            case Right((udtTree, sels)) => mkSelector(mkFieldSelectorOf(t1, r), fun, udtTree, sels)
          }

          case KeySelection(t1, r, fun, ret) => ret match {

            case Left(errs)             => errs.foreach { err => Error.report("Error analyzing KeySelector[" + mkFunctionType(t1, r) + "]: " + err) }; tree

            case Right((udtTree, sels)) => mkSelector(mkKeySelectorOf(t1, r), fun, udtTree, sels)
          }

          case tree => tree
        }
      }

      private def mkSelector(classTpe: Type, fun: Tree, udt: Tree, sels: List[List[String]]): Tree = {

        val selsString = "(" + (sels map {
          case Nil => "<all>"
          case sel => sel.mkString(".")
        } mkString (", ")) + ")"

        Debug.report("%-80s%-20s        %-200s".format("Analyzed " + classTpe + ": ", selsString, fun))

        val selsTree = mkSeq(sels map { sel => mkSeq(sel map { Literal(_) }) })

        localTyper.typed {
          New(TypeTree(classTpe), List(List(udt, selsTree)))
        }
      }
    }
  }
}

