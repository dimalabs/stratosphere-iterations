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

/**
 * Warning: experimental!
 * This was the first attempt at a tree reducer. It doesn't handle all kinds of statements. 
 * More importantly, it doesn't properly deal with side effects (eg, assignment).
 */

/*
package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global

trait TreeReducers { this: HasGlobal with TreeGenerators =>

  import global._

  object TreeReducer {

    class Environment(symFactory: Environment.SymbolFactory, defs: Map[Symbol, Tree], parents: Set[Symbol]) {

      private def filter(tree: Tree)(p: Tree => Boolean): List[Tree] = {
        val ft = new Traverser {
          val hits = new collection.mutable.ListBuffer[Tree]
          override def traverse(t: Tree) {
            if (p(t)) hits += t
            t match {
              case Record(members)      => members map { _._2 } foreach { super.traverse(_) }
              case ReductionError(t, _) => super.traverse(t)
              case t                    => super.traverse(t)
            }
          }
        }
        ft.traverse(tree)
        ft.hits.toList
      }

      def makeSymbol(owner: Symbol, name: String): Symbol = symFactory.makeSymbol(owner, name)

      def update(owner: Symbol, root: Tree): Option[Environment] = {

        if (parents.contains(owner)) {
          None
        } else {
          val defTrees = filter(root) {
            case t if t.symbol == null || t.symbol == NoSymbol          => false
            case t if t.symbol.isConstructor                            => false
            case t if t.symbol.isCaseAccessor                           => false
            case ValDef(_, _, _, EmptyTree)                             => false
            case DefDef(_, _, _, _, _, EmptyTree)                       => false
            case t: ValOrDefDef if t.symbol.isFinal || t.symbol.isLocal => true
            case _                                                      => false
          }

          val roots = root match {
            case Function(params, _) => params map { p => Ident(p.symbol) setPos root.pos }
            case _                   => Nil
          }

          val newDefs = (defTrees ++ roots) map { t => (t.symbol, t) } toMap

          val newParents = owner match {
            case null     => parents
            case NoSymbol => parents
            case _        => parents + owner
          }

          Some(new Environment(symFactory, defs ++ newDefs, newParents))
        }
      }

      def dereference(tree: Tree): Option[Tree] = tree match {
        case Ident(_) | Select(_, _) => defs.get(tree.symbol)
        case _                       => None
      }
    }

    object Environment {

      def Empty = new Environment(new SymbolFactory, Map(), Set())
      def apply(root: Tree): Environment = Empty.update(NoSymbol, root).get

      class SymbolFactory private[Environment] {
        import scala.collection.mutable
        private val symCache = {
          val initial = mutable.Map[Symbol, mutable.Map[String, Symbol]]()
          initial withDefault { owner =>
            val inner = mutable.Map[String, Symbol]()
            initial(owner) = inner
            inner
          }
        }

        def makeSymbol(owner: Symbol, name: String): Symbol = symCache(owner).getOrElseUpdate(name, { owner.newValue(owner.pos, name) })
      }
    }

    case class Record(members: List[(Name, Tree)]) extends TermTree
    case class ReductionError(tree: Tree, errMsg: String) extends Tree

    def toString(tree: Tree) = {

      val scrubber = new Transformer {
        override def transform(tree: Tree): Tree = super.transform {
          tree match {
            case Record(members) => Apply(Select(New(TypeTree(tree.tpe)), "<init>"), members map { _._2 })
            case _               => tree
          }
        }
      }
      
      scrubber.transform(tree).toString
    }

    def reduce(tree: Tree, env: TreeReducer.Environment): Tree = (new TreeReducer(env)).transform(tree)
  }

  protected class TreeReducer(protected val env: TreeReducer.Environment) extends Transformer {

    private val treeGen = new TreeGenerator {}

    import TreeReducer._
    import treeGen._

    override def transform(tree: Tree): Tree = tree match {

      case _: ReductionError        => tree

      case Function(Nil, body)      => transform(body)
      case _: Function              => tree

      //case Reference(err @ ReductionError(tree, _))     => { super.transform(tree); err }
      case Reference(target: Ident) => target // Idents in the environment are roots
      case Reference(target)        => transform(target)

      case Record(members)          => Record(members map { m => (m._1, transform(m._2)) }) setType tree.tpe

      //case CtorFunction(err @ ReductionError(tree, _))  => { super.transform(tree); err }
      case CtorFunction(fun)        => transform(fun)

      //case MatchFunction(err @ ReductionError(tree, _)) => { super.transform(tree); err }
      case MatchFunction(fun)       => transform(fun)

      case DefFunction(fun)         => transform(fun)

      case Block(_, expr)           => transform(expr)

      case Typed(fun, _)            => transform(fun)
      case TypeApply(fun, _)        => transform(fun)

      case Select(src, member) => transform(src) match {

        case fun: Function if member.toString == "apply" => fun

        case Record(sels) => sels.find(_._1.toString == member.toString) match {
          case Some((_, sel)) => sel
          case None           => ReductionError(tree, "Member not defined")
        }

        case src @ (Ident(_) | Select(_, _)) => Select(src, member)
        case src: ReductionError             => src
        case src                             => ReductionError(Select(src, member), "Invalid selection target")
      }

      //case Apply(Select(fun, Name("apply")), args) if isLambda(fun) => transform(Apply(fun, args))

      case Apply(fun, args) => transform(fun) match {

        case fun @ Function(params, body) => {

          val sub = new TreeSubstituter(params map { _.symbol }, args map transform) {
            override def transform(tree: Tree): Tree = tree match {
              case err: ReductionError => err
              case Record(members)     => Record(members map { m => (m._1, super.transform(m._2)) }) setType tree.tpe
              case tree                => super.transform(tree)
            }
          }

          // Beta reduction
          val expr = sub.transform(body)

          env.update(fun.symbol, expr) match {
            case Some(newEnv) => TreeReducer.reduce(expr, newEnv)
            case None         => ReductionError(tree, "Recursion detected")
          }
        }

        case err: ReductionError => err
        case nonFun              => ReductionError(nonFun, "Cannot apply non-function expression of type " + getTreeClassName(tree))
      }

      case _ => { super.transform(tree); ReductionError(tree, "Unsupported expression of type " + getTreeClassName(tree)) }
    }
    
    private def getTreeClassName(tree: Tree): String = {
      val name = tree.getClass.getName
      val idx = math.max(name.lastIndexOf('$'), name.lastIndexOf('.')) + 1
      name.substring(idx)
    }

    private object Name {
      def unapply(name: Name): Option[String] = Some(name.toString)
    }

    // Gets the definition of the target of an Ident or Select from the environment.
    private object Reference {
      def unapply(tree: Tree): Option[Tree] = env.dereference(tree)
    }

    private def isLambda(tree: Tree): Boolean = definitions.FunctionClass.exists(fun => tree.tpe <:< fun.tpe)

    // Create an anonymous function with a symbol. The symbol allows for
    // recursion detection even when the recursion is not anchored by
    // a named definition (for example, the Y combinator).
    private def mkFunction(owner: Symbol, paramss: List[List[ValDef]], body: Tree): Function = {
      val sym = env.makeSymbol(owner, "anonfun$")

      val fun = paramss match {
        case Nil            => Function(Nil, body)
        case params :: Nil  => Function(params, body)
        case params :: rest => Function(params, mkFunction(sym, rest, body))
      }

      fun setPos owner.pos setSymbol sym
    }

    private object DefFunction {

      def unapply(tree: Tree): Option[Tree] = tree match {

        case ValDef(_, _, _, body)             => Some(mkFunction(tree.symbol, Nil, body))
        case DefDef(_, _, _, paramss, _, body) => Some(mkFunction(tree.symbol, paramss, body))
        case _                                 => None
      }
    }

    private object CtorFunction {
      def unapply(tree: Tree): Option[Tree] = tree match {

        // Convert case class primary constructor to Record
        case Select(New(tpt), _) => {

          tree.symbol match {

            case sym if !sym.isPrimaryConstructor => Some(ReductionError(tree, "Non-primary constructor"))
            case sym if !sym.owner.isCaseClass    => Some(ReductionError(tree, "Non-case-class constructor"))

            case _ => {

              val fields = tree.symbol.owner.caseFieldAccessors
              val funParams = fields map { sym => ValDef(sym) setSymbol sym setPos tree.pos }

              val funElems = fields map { sym => (sym.name, Ident(sym) setPos tree.pos) }
              val fun = mkFunction(tree.symbol, List(funParams), Record(funElems) setType tpt.tpe setPos tree.pos)

              Some(fun)
            }
          }
        }

        case _ => None
      }
    }

    private object MatchFunction {

      def unapply(tree: Tree): Option[Tree] = tree match {

        // Eta-expand the CaseDef's body and apply the selections indicated by its pattern 
        case Match(arg, List(cd @ CaseDef(CasePattern(bindings), EmptyTree, body))) => {

          val (syms, sels) = bindings.unzip
          val funParams = syms map { sym => ValDef(sym) setSymbol sym setPos tree.pos }
          val funArgs = sels map { path => mkSelect(arg, path: _*) setPos tree.pos }
          val fun = Function(funParams, body)

          Some(Apply(fun, funArgs))
        }

        case Match(_, List(CaseDef(pat, EmptyTree, _))) => Some(ReductionError(pat, "Case pattern is too complex"))
        case Match(_, List(CaseDef(_, guard, _)))       => Some(ReductionError(guard, "Case pattern is guarded"))
        case Match(_, _ :: _ :: _)                      => Some(ReductionError(tree, "Match contains more than one case"))

        case _                                          => None
      }

      private object CasePattern {

        def unapply(tree: Tree): Option[(List[(Symbol, Seq[String])])] = tree match {

          case Apply(MethodTypeTree(params), binds) => {

            val exprs = params.zip(binds) map {
              case (p, CasePattern(inners)) => Some(inners map { case (sym, path) => (sym, p.name.toString +: path) })
              case _                        => None
            }

            if (exprs.forall(_.isDefined))
              Some(exprs.flatten.flatten)
            else
              None
          }

          case Ident(_)                     => Some(List((tree.symbol, Seq())))
          case Bind(_, Ident(_))            => Some(List((tree.symbol, Seq())))
          case Bind(_, CasePattern(inners)) => Some((tree.symbol, Seq()) +: inners)
          case _                            => None
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
    }
  }
}
*/