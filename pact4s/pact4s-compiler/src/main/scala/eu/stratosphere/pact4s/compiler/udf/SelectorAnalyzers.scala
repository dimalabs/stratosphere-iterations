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

trait SelectorAnalyzers { this: Pact4sPlugin =>

  import global._
  import defs._

  trait SelectorAnalyzer extends UDTGenSiteParticipant { this: TypingTransformer with TreeGenerator with Logger =>

    private val logger: Logger = this

    protected object FieldSelector {

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, List(t1, r)), List(fun)) if view.symbol == unanalyzedFieldSelector => {

          val sels = getUDT(t1.tpe).right.flatMap {
            case (udt, desc) => {
              val ret = getSelector(fun).right flatMap { sels =>

                val errs = sels flatMap { sel => chkSelector(desc, sel.head, sel.tail) }
                Either.cond(errs.isEmpty, sels map { _.tail }, errs)
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

              val selsString = "(" + (sels map {
                case Nil => "<all>"
                case sel => sel.mkString(".")
              } mkString (", ")) + ")"

              val msg = "%-80s%-20s        %-200s".format("Analyzed FieldSelector[" + mkFunctionType(t1.tpe, r.tpe) + "]:", selsString, fun)
              Debug.report(msg)

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

        udtWithDesc.toRight(List("Missing UDT[" + tpe + "]"))
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

      private def getSelector(tree: Tree): Either[List[String], List[List[String]]] = tree match {

        case Function(List(p), body) => getSelector(body, Map(p.symbol -> Nil)) match {
          case err @ Left(_) => err
          case Right(sels)   => Right(sels map { sel => p.name.toString +: sel })
        }

        case _ => Left(List("Expected lambda expression literal but found " + tree.getSimpleClassName))
      }

      private def getSelector(tree: Tree, roots: Map[Symbol, List[String]]): Either[List[String], List[List[String]]] = tree match {

        case SimpleMatch(body, bindings)                => getSelector(body, roots ++ bindings)

        case Match(_, List(CaseDef(pat, EmptyTree, _))) => Left(List("case pattern is too complex"))
        case Match(_, List(CaseDef(_, guard, _)))       => Left(List("case pattern is guarded"))
        case Match(_, _ :: _ :: _)                      => Left(List("match contains more than one case"))

        case TupleCtor(args) => {

          val (errs, sels) = args.map(arg => getSelector(arg, roots)).partition(_.isLeft)

          errs match {
            case Nil => Right(sels.map(_.right.get).flatten)
            case _   => Left(errs.map(_.left.get).flatten)
          }
        }

        case Apply(Select(New(tpt), _), _) => Left(List("constructor call on non-tuple type " + tpt.tpe))

        case Ident(name) => roots.get(tree.symbol) match {
          case Some(sel) => Right(List(sel))
          case None      => Left(List("unexpected identifier " + name))
        }

        case Select(src, member) => getSelector(src, roots) match {
          case err @ Left(_)    => err
          case Right(List(sel)) => Right(List(sel :+ member.toString))
          case _                => Left(List("unsupported selection"))
        }

        case _ => Left(List("unsupported construct of kind " + tree.getSimpleClassName))

      }

      private object SimpleMatch {

        def unapply(tree: Tree): Option[(Tree, Map[Symbol, List[String]])] = tree match {

          case Match(arg, List(cd @ CaseDef(CasePattern(bindings), EmptyTree, body))) => Some((body, bindings))
          case _ => None
        }

        private object CasePattern {

          def unapply(tree: Tree): Option[Map[Symbol, List[String]]] = tree match {

            case Apply(MethodTypeTree(params), binds) => {

              val exprs = params.zip(binds) map {
                case (p, CasePattern(inners)) => Some(inners map { case (sym, path) => (sym, p.name.toString +: path) })
                case _                        => None
              }

              if (exprs.forall(_.isDefined))
                Some(exprs.flatten.flatten.toMap)
              else
                None
            }

            case Ident(_) | Bind(_, Ident(_)) => Some(Map(tree.symbol -> Nil))
            case Bind(_, CasePattern(inners)) => Some(inners + (tree.symbol -> Nil))
            case _                            => None
          }
        }

        private object MethodTypeTree {
          def unapply(tree: Tree): Option[List[Symbol]] = tree match {
            case _: TypeTree => tree.tpe match {
              case MethodType(params, _) => Some(params)
              case _                     => None
            }
            case _ => None
          }
        }
      }

      private object TupleCtor {

        def unapply(tree: Tree): Option[List[Tree]] = tree match {
          case Apply(Select(New(tpt), _), args) if isTupleTpe(tpt.tpe) => Some(args)
          case _                                                       => None
        }

        private def isTupleTpe(tpe: Type): Boolean = definitions.TupleClass.contains(tpe.typeSymbol)
      }
    }
  }
}

