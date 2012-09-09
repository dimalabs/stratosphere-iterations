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

trait FallbackBinders { this: Pact4sPlugin =>

  import global._
  import defs._

  trait FallbackBinder { this: TypingTransformer with TreeGenerator with Logger =>

    protected object DefaultUDF {

      private val unanalyzedUDFs = Set(unanalyzedUDF1, unanalyzedUDF2)

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, tpeTrees), List(fun)) if unanalyzedUDFs.contains(view.symbol) => {

          val tpes = tpeTrees map { _.tpe }

          inferImplicitInsts(tpes map { tpe => mkUdtOf(unwrapIter(tpe)) }) match {

            case Left(errs) => {
              Error.report("Could not infer default UDF[" + mkFunctionType(tpes: _*) + "] @ " + curTree.id + " Missing UDTs: " + errs.mkString(", "))
              None
            }

            case Right(udts) => {
              //Debug.report("Inferred default for UDF[" + mkFunctionType(tpes: _*) + "] @ " + curTree.id)

              val (owner, kind) = getDefaultUDFKind(tpes)
              val factory = mkSelect("eu", "stratosphere", "pact4s", "common", "analyzer", owner, kind)
              Some(Apply(Apply(factory, List(fun)), udts))
            }
          }
        }

        case _ => None
      }

      // TODO (Joe): Find a better way to figure out what kind of default UDF we want.
      // Going by whether the type signature includes an Iterator prevents supporting
      // Iterator as a List variant for UDT generation.
      private def getDefaultUDFKind(tpes: List[Type]): (String, String) = (tpes: @unchecked) match {
        case List(t1, r) if isIter(t1) => ("AnalyzedUDF1", "defaultIterT")
        case List(t1, r) if isIter(r) => ("AnalyzedUDF1", "defaultIterR")
        case List(t1, r) => ("AnalyzedUDF1", "default")
        case List(t1, t2, r) if isIter(t1) && isIter(t2) && isIter(r) => ("AnalyzedUDF2", "defaultIterTR")
        case List(t1, t2, r) if isIter(t1) && isIter(t2) => ("AnalyzedUDF2", "defaultIterT")
        case List(t1, t2, r) if isIter(r) => ("AnalyzedUDF2", "defaultIterR")
        case List(t1, t2, r) => ("AnalyzedUDF2", "default")
      }
    }
  }
}

