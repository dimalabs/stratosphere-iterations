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

trait SelectorAnalyzers { this: Pact4sPlugin =>

  import global._
  import defs._

  trait SelectorAnalyzer extends UDTGenSiteParticipant { this: TypingTransformer with TreeGenerator with Logger =>

    protected object FieldSelector {

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, List(t1, r)), List(fun)) if view.symbol == unanalyzedFieldSelector => {

          val sels = getUDT(t1.tpe).right.flatMap {
            case (udt, desc) => {
              val ret = getSelectors(fun).right flatMap { sels =>
                val errs = sels flatMap chkSelector(desc)
                Either.cond(errs.isEmpty, sels, errs)
              }
              ret.right map { (udt, _) }
            }
          }

          sels match {
            
            case Left(errs) => {
              errs.foreach { err => Error.report("Error analyzing FieldSelector[" + mkFunctionType(t1.tpe, r.tpe) + "]: " + err) }
              None
            }

            case Right((udt, sels)) => {

              Debug.report("Analyzed FieldSelector[" + mkFunctionType(t1.tpe, r.tpe) + "]: { " + sels.map(_.mkString(".")).mkString(", ") + " }")

              def mkSeq(items: List[Tree]): Tree = Apply(mkSelect("scala", "collection", "Seq", "apply"), items)
              val selsTree = mkSeq(sels map { sel => mkSeq(sel map { Literal(_) }) })

              val factory = mkSelect("eu", "stratosphere", "pact4s", "common", "analyzer", "AnalyzedFieldSelector", "apply")

              Some(Apply(TypeApply(factory, List(TypeTree(t1.tpe), TypeTree(r.tpe))), List(udt, selsTree)))
            }
          }
        }

        case _ => None
      }

      private def getUDT(tpe: Type): Either[List[String], (Tree, UDTDescriptor)] = {
        
        val udt = inferImplicitInst(mkUdtOf(tpe))
        val udtWithDesc = udt flatMap { ref => getUDTDescriptors(unit) get ref.symbol map ((ref, _)) }
        
        udtWithDesc.toRight(List("Missing UDT: " + tpe))
      }

      private def getSelectors(fun: Tree): Either[List[String], List[List[String]]] = Left(List("getSelectors Not Implemented"))

      private def chkSelector(udt: UDTDescriptor)(sel: List[String]): List[String] = List("chkSelector Not Implemented")
    }
  }
}

