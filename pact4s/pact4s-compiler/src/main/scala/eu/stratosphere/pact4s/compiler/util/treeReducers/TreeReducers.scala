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

package eu.stratosphere.pact4s.compiler.util.treeReducers

import eu.stratosphere.pact4s.compiler.util._

trait TreeReducers extends TreeReducerEnvironments with TreeReducerImpls { this: HasGlobal with TreeGenerators =>

  import global._

  object TreeReducer extends TreeReducerEnvironment {

    object SymbolFactory {
      import scala.collection.mutable
      private val symCache = {
        val initial = mutable.Map[Symbol, mutable.Map[String, Symbol]]()
        initial withDefault { scope =>
          val inner = mutable.Map[String, Symbol]()
          initial(scope) = inner
          inner
        }
      }

      def makeSymbol(scope: Symbol, name: String): Symbol = symCache(scope).getOrElseUpdate(name, { scope.cloneSymbol(scope) })

      def makeAnonFun(owner: Symbol): Symbol = makeSymbol(owner, "anonfun$")
      def makeInstanceOf(classSym: Symbol): Symbol = makeSymbol(classSym, "inst$")
      def makeCtorFlag(classSym: Symbol): Symbol = makeSymbol(classSym, "init$")
    }

    object TypeOf {
      private val typeOfSym = NoSymbol.newValue("typeOf$")
      def update(env: Environment, typeSym: Symbol): Unit = env(typeOfSym) = TypeTree(typeSym.thisType)
      def apply(env: Environment): Symbol = env.get(typeOfSym) match {
        case Some(tpt) => tpt.symbol
        case None      => NoSymbol
      }
    }

    abstract class Closure extends SymTree {

      def unpacked: (Environment, List[ValDef], Tree)

      def getParams: List[List[ValDef]] = {
        val (_, params, body) = unpacked
        params :: (body match {
          case fun: Closure => fun.getParams
          case _            => Nil
        })
      }

      override def productArity = 3
      override def productElement(n: Int): Any = n match {
        case 0 => unpacked._1
        case 1 => unpacked._2
        case 2 => unpacked._3
        case _ => throw new IndexOutOfBoundsException
      }
      override def canEqual(that: Any): Boolean = that match {
        case _: Closure => true
        case _          => false
      }
    }

    case class NonReducible(reason: ReductionError, expr: Tree) extends TermTree

    sealed abstract class ReductionError
    object ReductionError {
      case object NoSource extends ReductionError
      case object Recursive extends ReductionError
      case object NonDeterministic extends ReductionError

      class CausedBy(val cause: NonReducible) extends ReductionError

      object CausedBy {
        def unapply(err: CausedBy): Option[NonReducible] = Some(err.cause)
        def apply(inner: NonReducible): ReductionError = inner.reason match {
          case reason: CausedBy => apply(reason.cause)
          case _                => new CausedBy(inner)
        }
      }
    }

    def reduce(units: List[CompilationUnit], strictBlocks: Boolean): Environment = {
      val env = Environment.Empty.makeChild
      units foreach { unit => reduce(unit.body, env, strictBlocks) }
      env
    }

    def reduce(tree: Tree, env: Environment, strictBlocks: Boolean): Tree = (new TreeReducer(strictBlocks)).reduce(tree, env)

    object Extractors {

      object ObjectDef {
        def unapply(tree: Tree) = tree match {
          case ClassDef(mods, name, tpts, impl) if tree.symbol.isModuleClass => Some((mods, name, tpts, impl))
          case _                                                             => None
        }
      }

      object LazyAccessorDef {
        def unapply(tree: Tree): Option[(Modifiers, TermName, List[TypeDef], List[List[ValDef]], Tree, Tree)] = tree match {
          case DefDef(mods, name, tparams, paramss, tpt, rhs) if tree.hasSymbolWhich(sym => sym.isLazyAccessor) => Some((mods, name, tparams, paramss, tpt, rhs))
          case _ => None
        }
      }

      object Closure {
        def unapply(fun: Closure): Some[(Environment, List[ValDef], Tree)] = Some(fun.unpacked)
      }
    }
  }
}