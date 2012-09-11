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
import eu.stratosphere.pact4s.compiler.util._

trait SelectorAnalyzers extends TreeReducers { this: Pact4sPlugin =>

  import global._
  import defs._
  import TreeReducer.ReductionError

  trait SymbolSnapshot { val snapshot: TreeReducer.Environment }

  trait SelectorAnalyzer extends UDTGenSiteParticipant { this: TypingTransformer with TreeGenerator with Logger with SymbolSnapshot =>

    protected object FieldSelector {

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, List(t1, r)), List(fun)) if view.symbol == unanalyzedFieldSelector => {

          val sels = getUDT(t1.tpe).right.flatMap {
            case (udt, desc) => {
              val ret = getSelector(fun, snapshot).right flatMap { sels =>

                val errs = sels flatMap { sel => chkSelector(desc, sel.head, sel.tail) } map { ReductionError(EmptyTree, _) }
                Either.cond(errs.isEmpty, sels map { _.tail }, errs)
              }
              ret.right map { (udt, _) }
            }
          }

          sels match {

            case Left(errs) => {
              errs.foreach { err => Error.report("Error analyzing FieldSelector[" + mkFunctionType(t1.tpe, r.tpe) + "]: " + err.errMsg + " - " + TreeReducer.toString(err.tree)) }
              None
            }

            case Right((udt, sels)) => {

              val selsString = "(" + (sels map {
                case Nil => "<all>"
                case sel => sel.mkString(".")
              } mkString (", ")) + ")"

              val msg = "%-80s%-20s        %-200s".format("Analyzed FieldSelector[" + mkFunctionType(t1.tpe, r.tpe) + "]:", selsString, fun)
              //Debug.report("Analyzed FieldSelector[" + mkFunctionType(t1.tpe, r.tpe) + "]: " + sels.map(_.mkString(".")).mkString(", ") + "        " + fun)
              Debug.report(msg)

              val selsTree = mkSeq(sels map { sel => mkSeq(sel map { Literal(_) }) })
              val factory = mkSelect("eu", "stratosphere", "pact4s", "common", "analyzer", "AnalyzedFieldSelector", "apply")

              Some(Apply(TypeApply(factory, List(TypeTree(t1.tpe), TypeTree(r.tpe))), List(udt, selsTree)))
            }
          }
        }

        case _ => None
      }

      private def getUDT(tpe: Type): Either[List[ReductionError], (Tree, UDTDescriptor)] = {

        val udt = inferImplicitInst(mkUdtOf(tpe))
        val udtWithDesc = udt flatMap { ref => getUDTDescriptors(unit) get ref.symbol map ((ref, _)) }

        udtWithDesc.toRight(List(ReductionError(EmptyTree, "Missing UDT: " + tpe)))
      }

      private def chkSelector(udt: UDTDescriptor, path: String, sel: List[String]): Option[String] = (udt, sel) match {
        case (_: OpaqueDescriptor, _)           => None
        case (_, Nil) if udt.isPrimitiveProduct => None
        case (_, Nil)                           => Some(path + ": " + udt.tpe + " is not a primitive or product of primitives")
        case (_, field :: rest) => udt.select(field) match {
          case None      => Some("member " + field + " is not a case accessor of " + path + ": " + udt.tpe)
          case Some(udt) => chkSelector(udt, path + "." + field, rest)
        }
      }

      private def getSelector(tree: Tree, env: TreeReducer.Environment): Either[List[ReductionError], List[List[String]]] = {

        def getResult(tree: Tree): Either[List[ReductionError], List[List[String]]] = tree match {

          case err: ReductionError => Left(List(err))

          case TreeReducer.Record(members) => {
            val (errs, sels) = members map { _._2 } map getResult partition { _.isLeft }
            errs match {
              case Nil => Right(sels map { _.right.get } flatten)
              case _   => Left(errs map { _.left.get } flatten)
            }
          }

          case Ident(name) => Right(List(List(name.toString)))

          case Select(src, member) => src match {

            case err: ReductionError => Left(List(err))

            case TreeReducer.Record(members) => members.find(m => m._1.toString == member.toString) match {
              case None      => Left(List(ReductionError(src, "Member " + member.toString + " not found")))
              case Some(src) => getResult(Select(src._2, member))
            }

            case Ident(name) => Right(List(List(name.toString, member.toString)))

            case src => getResult(src).right flatMap {
              case List(sel) => Right(List(sel :+ member.toString))
              case sels      => Left(List(ReductionError(EmptyTree, "Unexpected selection target: " + sels)))
            }
          }

          case _ => Left(List(ReductionError(tree, "Unexpected result")))
        }

        TreeReducer.reduce(tree, env) match {

          case fun @ Function(params, _) => {
            val args = params map { p => Ident(p.symbol) }
            getResult(TreeReducer.reduce(Apply(fun, args), env.update(NoSymbol, fun).get))
          }

          case err: ReductionError => Left(List(err))

          case nonFun              => Left(List(ReductionError(nonFun, "Unexpected result")))
        }
      }
    }
  }
}

