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

  trait SymbolSnapshot { val snapshot: Map[Symbol, Tree]; def mkSnapshot(tree: Tree): Map[Symbol, Tree] }

  trait SelectorAnalyzer extends UDTGenSiteParticipant { this: TypingTransformer with TreeGenerator with Logger with SymbolSnapshot =>

    protected object FieldSelector {

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, List(t1, r)), List(fun)) if view.symbol == unanalyzedFieldSelector => {

          val sels = getUDT(t1.tpe).right.flatMap {
            case (udt, desc) => {
              val ret = getSelector(fun, Set(), snapshot).getResult.right flatMap { sels =>

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

              Debug.report("Analyzed FieldSelector[" + mkFunctionType(t1.tpe, r.tpe) + "]: " + sels.map(_.mkString(".")).mkString(", "))

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

      private def chkSelector(udt: UDTDescriptor, path: String, sel: List[String]): Option[String] = (udt, sel) match {
        case (_: OpaqueDescriptor, _)           => None
        case (_, Nil) if udt.isPrimitiveProduct => None
        case (_, Nil)                           => Some(path + ": " + udt.tpe + " is not a primitive or product of primitives")
        case (_, field :: rest) => udt.select(field) match {
          case None      => Some("member " + field + " is not a case accessor of " + path + ": " + udt.tpe)
          case Some(udt) => chkSelector(udt, path + "." + field, rest)
        }
      }

      private def getSelector(tree: Tree, roots: Set[Symbol], env: Map[Symbol, Tree]): Selector = {

        object PatMatch {
          def unapply(tree: Tree): Option[Tree] = tree match {
            case Match(arg, List(CaseDef(pat @ PatType(substs), EmptyTree, body))) => {
              val (syms, paths) = substs.unzip
              val funParams = syms map { sym => ValDef(sym) setSymbol sym setPos tree.pos }
              val funArgs = paths map { path => mkSelect(arg, path: _*) setPos tree.pos }
              Some(Apply(Function(funParams, body), funArgs) setPos tree.pos)
            }
            case _ => None
          }

          object PatType {
            def unapply(tree: Tree): Option[(List[(Symbol, Seq[String])])] = tree match {
              case Apply(MethodTypeTree(params), binds) => {
                val exprs = params.zip(binds) map {
                  case (p, PatType(inners)) => Some(addPrefix(p.name.toString, inners))
                  case _                    => None
                }

                if (exprs.forall(_.isDefined))
                  Some(exprs.flatten.flatten)
                else
                  None
              }
              case Ident(_)                 => Some(List((tree.symbol, Seq())))
              case Bind(_, Ident(_))        => Some(List((tree.symbol, Seq())))
              case Bind(_, PatType(inners)) => Some((tree.symbol, Seq()) +: inners)
              case _                        => None
            }

            private def addPrefix(prefix: String, inners: List[(Symbol, Seq[String])]) = inners map { case (sym, path) => (sym, prefix +: path) }
          }
        }

        object MethodTypeTree {
          def unapply(tree: Tree): Option[List[Symbol]] = tree match {
            case _: TypeTree => tree.tpe match {
              case MethodType(params, _) => Some(params)
              case _                     => None
            }
            case _ => None
          }
        }

        object Reference {
          def unapply(tree: Tree): Option[Tree] = tree match {
            case _: Ident                  => env.get(tree.symbol)
            case _: Select                 => env.get(tree.symbol)
            case TypeApply(fun: Ident, _)  => unapply(fun)
            case TypeApply(fun: Select, _) => unapply(fun)
            case _                         => None
          }
        }

        object Ctor {
          def unapply(tree: Tree): Option[Tree] = tree match {
            case Select(New(_), _) if tree.symbol.isPrimaryConstructor && tree.symbol.owner.isCaseClass => {
              val fields = tree.symbol.owner.caseFieldAccessors
              val funParams = fields map { sym => ValDef(sym) setSymbol sym setPos tree.pos }

              // Abusing the Bind and Alternative subclasses because they have
              // the parameter types we need to form a list of labeled trees.
              // The Binds don't have symbols, so they will survive eval'ing the 
              // Function, while the Idents will be replaced by args from Apply.
              val funElems = fields map { sym => Bind(sym.name, Ident(sym) setPos tree.pos) setPos tree.pos }
              val fun = Function(funParams, Alternative(funElems) setPos tree.pos) setPos tree.pos

              Some(fun)
            }
            case Select(New(_), name) if tree.symbol.isConstructor => Some(EmptyTree)
            case _                                                 => None
          }
        }

        tree match {

          case Reference(EmptyTree) => {
            //Debug.report("getSelector(" + tree + ") failed: illegal reference")
            ComplexSelector(tree)
          }
          case Ctor(EmptyTree) => {
            //Debug.report("getSelector(" + tree + ") failed: non-default constructor")
            ComplexSelector(tree)
          }
          case Reference(target)                          => getSelector(tree, target, roots, env + (tree.symbol -> EmptyTree))
          case Ctor(fun)                                  => getSelector(tree, fun, roots, env)
          case PatMatch(fun)                              => getSelector(tree, fun, roots, env)
          case Typed(expr, _)                             => getSelector(tree, expr, roots, env)
          case Block(_, expr)                             => getSelector(tree, expr, roots, env)
          case ValDef(_, _, _, body)                      => getSelector(tree, body, roots, env)
          case DefDef(m, n, tps, paramss, tpt, body)      => getSelector(tree, paramss.foldRight(body) { (params, body) => Function(params, body) setPos tree.pos }, roots, env)
          case Function(Nil, body)                        => getSelector(tree, body, roots, env)
          case Ident(name) if roots.contains(tree.symbol) => getSelector(tree, RootSelector(name.toString))
          case Ident(name) => {
            //Debug.report("getSelector(" + tree + ") failed: illegal identifier")
            ComplexSelector(tree)
          }
          case Select(src, member) => {
            //Debug.report("getSelector(" + tree + ") = SimpleSelector(getSelector(" + src + "), " + member + ")");
            getSelector(src, roots, env) match {
              case sel: ComplexSelector => sel
              
              case sel @ CompositeSelector(sels) => sels.find(_._1 == member.toString) match {
                case Some((_, sel)) => sel
                case None => {
                  //Debug.report("getSelector(" + tree + ") failed: " + sel + " does not define member " + member)
                  ComplexSelector(tree)
                }
              }
              case sel => SimpleSelector(sel, member.toString)
            }
          }
          case Alternative(elems) => {
            val elemsStr = elems map { case Bind(name, arg) => (name.toString, "getSelector(" + arg + ")") }
            //Debug.report("getSelector(" + tree + ") = CompositeSelector(" + elemsStr.mkString(", ") + ")")
            CompositeSelector(elems map { case Bind(name, arg) => (name.toString, getSelector(arg, roots, env)) })
          }
          case Function(params, body) => getSelector(tree, LazySelector(params map { _.symbol }, body, roots, env))
          case Apply(fun, args) => {
            //Debug.report("getSelector(" + tree + ") = getSelector(" + fun + ").eval(" + args.mkString(", ") + ")")
            getSelector(fun, roots, env) match {
              case sel: ComplexSelector => sel
              case sel: LazySelector    => sel.eval(args)
              case sel => {
                //Debug.report("getSelector(" + tree + ") failed: expected LazySelector but found " + sel)
                ComplexSelector(fun)
              }
            }
          }
          case _ => {
            //Debug.report("getSelector(" + tree + ") failed: no matching pattern")
            ComplexSelector(tree)
          }
        }
      }

      private def getSelector(from: Tree, to: Tree, roots: Set[Symbol], env: Map[Symbol, Tree]): Selector = {
        //Debug.report("getSelector(" + from + ") = getSelector(" + to + ")")
        getSelector(to, roots, env)
      }

      private def getSelector(from: Tree, to: Selector): Selector = {
        //Debug.report("getSelector(" + from + ") = " + to)
        to
      }

      abstract sealed class Selector {
        def getResult: Either[List[String], List[List[String]]]
      }

      case class ComplexSelector(tree: Tree) extends Selector {
        
        private def treeKind = {
          var name = tree.getClass.getName
          if (name.endsWith("$")) name = name.substring(0, name.length - 1)
          val idx = math.max(name.lastIndexOf('$'), name.lastIndexOf('.')) + 1
          name.substring(idx)
        }
        
        override def getResult = {
          val loc = "%s[%s:%s]".format(tree.pos.source, tree.pos.line, tree.pos.column)
          Left(List(treeKind + " expression is too complex @ " + loc))
        }
      }

      case class LazySelector(params: List[Symbol], body: Tree, roots: Set[Symbol], env: Map[Symbol, Tree]) extends Selector {
        def eval(args: List[Tree]) = {
          val subst = (new TreeSubstituter(params, args)).transform(body)
          val (dead, alive) = env.partition(_._2.isEmpty)
          //Debug.report(this + ".eval(" + args.mkString(", ") + ") = getSelector(" + subst + ")")
          getSelector(subst, roots, (alive ++ mkSnapshot(subst)) ++ dead)
        }

        private def eval(): Selector = {
          //Debug.report(this + ".eval()")
          getSelector(body, roots ++ params, env) match {
            case LazySelector(params, body, _, _) => ComplexSelector(Function(params map { sym => ValDef(sym) setSymbol sym }, body))
            case sel                              => sel
          }
        }

        def getResult = eval().getResult

        override def toString = "LazySelector(" + params.map(_.name) + ", " + body + ")"
      }

      case class RootSelector(name: String) extends Selector {
        override def getResult = Right(List(List(name)))
      }

      case class SimpleSelector(source: Selector, member: String) extends Selector {

        override def getResult = source.getResult.right flatMap {
          case List(path) => Right(List(path :+ member))
          case paths      => Left(List(" found unexpected composite (" + paths.map(_.mkString(".")).mkString(", ") + ")"))
        }
      }

      case class CompositeSelector(sels: List[(String, Selector)]) extends Selector {

        override def getResult = {
          val (errs, rets) = sels map { _._2.getResult } partition { _.isLeft }

          errs match {
            case Nil => Right(rets flatMap { ret => ret.right.get })
            case _   => Left(errs flatMap { err => err.left.get })
          }
        }
      }
    }
  }
}

