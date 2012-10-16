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
import eu.stratosphere.pact4s.compiler.util.treeReducers._

trait SelectorAnalyzers { this: Pact4sPlugin =>

  import global._
  import defs._
  import TreeReducerHelpers._

  trait SelectorAnalyzer extends UDTGenSiteParticipant { this: TypingTransformer with TreeGenerator with Logger with TreeReducer =>

    private val logger: Logger = this
    val snapshot: Environment

    protected object FieldSelector {

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, List(t1, r)), List(fun)) if view.symbol == unanalyzedFieldSelector => {

          val sels = getUDT(t1.tpe).right.flatMap {
            case (udt, desc) => {
              val ret = getSelector(fun, desc).right flatMap { sels =>

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

      private def getUDT(tpe: Type): Either[List[NonReducible], (Tree, UDTDescriptor)] = {

        val udt = inferImplicitInst(mkUdtOf(tpe))
        val udtWithDesc = udt flatMap { ref => getUDTDescriptors(unit) get ref.symbol map ((ref, _)) }

        udtWithDesc.toRight(List(NonReducible(ReductionError.NoSource, EmptyTree)))
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

      private def getSelector(tree: Tree, desc: UDTDescriptor): Either[List[String], List[List[String]]] = {

        try {
          val env = treeReducer.build(snapshot.copy, curPath.reverse.toList)

          treeReducer.reduce(tree, env, true) match {

            case fun @ Closure(_, List(param), _) => {
              val root = mkRootParams(desc, Seq(param.symbol.name.toString), env)
              val strRoots = root.toMap map {
                case (k, v: Environment) => Tag(v) match {
                  case Some(sel) => sel
                  case None      => "[" + k.name + " = " + v + "]"
                }
                case (k, v) => "[" + k.name + " = " + v + "]"
              }

              val ret = treeReducer.reduce(Apply(fun, List(root)), env, true)
              getSelectorFromReduction(ret)
            }

            case nonFun => Left(List(NonReducible(ReductionError.Unexpected, nonFun).toString))
          }
        } catch {
          case ex => {
            val pathDefs = curPath.reverse.filter {
              case _: MemberDef => true
              case _            => false
            }
            val path = pathDefs.map(_.symbol.name).mkString(".")
            Error.report("Path: " + path)
            reporter.flush
            treeBrowser.browse(curPath.last)
            throw ex
          }
        }
      }

      private def getSelectorFromReduction(tree: Tree): Either[List[String], List[List[String]]] = tree match {

        case err: NonReducible => Left(List(err.toString))

        case env: Environment => {

          val instSym = env.symbol match {
            case NoSymbol => NoSymbol
            case sym      => SymbolFactory.makeInstanceOf(sym.owner)
          }

          Tag(env) match {

            case Some(sel) => Right(List(sel.split('.').toList))

            case None if env.symbol eq instSym => {
              val paramSyms = env.symbol.owner.primaryConstructor.paramss.flatten map { sym => sym.getter(env.symbol.owner) }
              val (errs, sels) = paramSyms map { sym => getSelectorFromReduction(env(sym)) } partition { _.isLeft }
              errs match {
                case Nil => Right(sels map { _.right.get } flatten)
                case _   => Left(errs map { _.left.get } flatten)
              }
            }

            case _ => {
              Warn.report("Result environment " + env.symbol.name.toString + " is not an instance: " + env.toMap.keys.map(_.name).mkString(", "))
              Left(List(NonReducible(ReductionError.Unexpected, tree).toString))
            }
          }
        }

        case _ => {
          Warn.report("Result is not an environment")
          Left(List(NonReducible(ReductionError.Unexpected, tree).toString))
        }
      }

      private def mkRootParams(desc: UDTDescriptor, path: Seq[String], scope: Environment): Environment = desc match {

        case _: PrimitiveDescriptor | _: BoxedPrimitiveDescriptor | _: ListDescriptor | _: RecursiveDescriptor => {
          val env = Environment.Empty.makeChild
          Tag(env) = path.mkString(".")
          env
        }

        case CaseClassDescriptor(_, _, ctorSym, _, getters) => {
          val ctor = Select(New(Ident(ctorSym.owner)), ctorSym)
          val args = getters map { case FieldAccessor(sym, _, _, desc) => mkRootParams(desc, path :+ sym.name.toString, scope) }
          val env = treeReducer.reduce(Apply(ctor, args.toList), scope, false) match {
            case env: Environment => env
            case _                => throw new UnsupportedOperationException("Unexpected result")
          }
          Tag(env) = path.mkString(".")
          env
        }

        case BaseClassDescriptor(_, tpe, getters, _) => {
          val classSym = tpe.typeSymbol
          val ctorSym = classSym.primaryConstructor

          val ctor = Select(New(Ident(classSym)), ctorSym)
          val args = ctorSym.paramss map {
            _ map { param =>
              getters.find(f => f.sym eq param) match {
                case None                                 => NonReducible(ReductionError.Synthetic, EmptyTree)
                case Some(FieldAccessor(sym, _, _, desc)) => mkRootParams(desc, path :+ sym.name.toString, scope)
              }
            }
          }

          val expr = args.foldLeft(ctor: Tree) { (fun, args) => Apply(fun, args) }

          val env = treeReducer.reduce(expr, scope, false) match {
            case env: Environment => env
            case _                => throw new UnsupportedOperationException("Unexpected result")
          }

          val ctorParamSyms = ctorSym.primaryConstructor.paramss.flatten.toSet

          for (FieldAccessor(sym, _, _, getter) <- getters.tail if !ctorParamSyms.contains(sym)) {
            env.setMember(sym, mkRootParams(getter, path :+ sym.name.toString, scope))
          }

          Tag(env) = path.mkString(".")
          env
        }

        case OpaqueDescriptor(_, tpe, _) => {
          val classSym = tpe.typeSymbol
          val ctorSym = classSym.primaryConstructor

          val ctor = Select(New(Ident(classSym)), ctorSym)
          val args = ctorSym.paramss map { _ map { _ => NonReducible(ReductionError.Synthetic, EmptyTree) } }
          val expr = args.foldLeft(ctor: Tree) { (fun, args) => Apply(fun, args) }

          val env = treeReducer.reduce(expr, scope, false) match {
            case env: Environment => env
            case _                => throw new UnsupportedOperationException("Unexpected result")
          }

          Tag(env) = path.mkString(".")
          env
        }
      }
    }
  }
}

