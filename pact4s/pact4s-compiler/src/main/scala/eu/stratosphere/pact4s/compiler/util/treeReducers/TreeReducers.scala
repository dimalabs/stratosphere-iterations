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
    }

    object TypeOf {
      private val typeOfSym = NoSymbol.newValue("typeOf$")
      def update(env: Environment, typeSym: Symbol): Unit = env(typeOfSym) = TypeTree(typeSym.thisType)
      def apply(env: Environment): Symbol = env.get(typeOfSym) match {
        case Some(tpt) => tpt.symbol
        case None      => NoSymbol
      }
    }

    object CtorFlag {
      def checkAndSet(env: Environment, classSym: Symbol): Boolean = {
        val flag = SymbolFactory.makeSymbol(classSym, "init$")
        val isFirst = !env.defines(flag)
        if (isFirst) env(flag) = EmptyTree
        isFirst
      }
    }

    object Tag {
      private val tagSym = NoSymbol.newValue("tag$")
      def update(env: Environment, value: String): Unit = env(tagSym) = Literal(value)
      def apply(env: Environment): Option[String] = env.get(tagSym) match {
        case Some(Literal(Constant(value: String))) => Some(value)
        case _                                      => None
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

    case class NonReducible(reason: ReductionError, expr: Tree) extends TermTree {
      def panic: Unit = {
        var envs: List[Environment] = Nil
        val findEnvs = new Traverser {
          import Extractors._
          import ReductionError._
          override def traverse(tree: Tree) = tree match {
            case env: Environment                    => envs = env :: envs
            case Closure(env, _, body)               => envs = env :: envs; traverse(body)
            case NonReducible(CausedBy(cause), expr) => traverse(cause); traverse(expr)
            case NonReducible(_, expr)               => traverse(expr)
            case tree                                => super.traverse(tree)
          }
        }
        findEnvs.traverse(this)
        envs foreach { _.panic }
      }

      override def toString = "NonReducible(" + reason.toString + "): " + expr
    }

    object NonReducible {
      def panicked(reason: ReductionError, expr: Tree): NonReducible = {
        val ret = NonReducible(reason, expr)
        ret.panic
        ret
      }
    }

    sealed abstract class ReductionError
    object ReductionError {
      case object NotImplemented extends ReductionError
      case object Unexpected extends ReductionError
      case object Synthetic extends ReductionError
      case object NoSource extends ReductionError
      case object Recursive extends ReductionError
      case object NonDeterministic extends ReductionError

      class CausedBy(val cause: NonReducible) extends ReductionError {
        override def toString = "CausedBy(" + cause + ")"
      }

      object CausedBy {
        def unapply(err: CausedBy): Option[NonReducible] = Some(err.cause)
        def apply(inner: NonReducible): ReductionError = inner.reason match {
          case reason: CausedBy => apply(reason.cause)
          case _                => new CausedBy(inner)
        }
      }
    }

    object EnvironmentBuilder {

      case object CurrentEnvironment extends Tree

      def build(env: Environment, path: List[Tree]): Environment = path match {

        case Nil => env

        case (pd: PackageDef) :: rest => env.get(pd.symbol) match {
          case Some(env: Environment) => build(env, rest)
          case _                      => throw new UnsupportedOperationException("Couldn't find package")
        }

        case (cd: ClassDef) :: rest => env.get(cd.symbol) match {
          case Some(inst: Environment) if cd.symbol.isModuleClass => build(inst, rest)
          case Some(classEnv: Environment) => {

            classEnv.get(cd.symbol.primaryConstructor) match {
              case Some(Extractors.Closure(classEnv, params, body)) => {
                val inst = classEnv.makeChild setSymbol SymbolFactory.makeInstanceOf(classEnv.symbol)
                TypeOf(inst) = classEnv.symbol
                val ctor = new Closure { override def unpacked = (inst, params, body) }
                ctor setSymbol cd.symbol.primaryConstructor
                val newExpr = cd.symbol.primaryConstructor.paramss.foldLeft(ctor: Tree) { (fun, params) => Apply(fun, params map { _ => NonReducible(ReductionError.Synthetic, EmptyTree) }) }
                reduce(newExpr, env, false)
                build(inst, rest)
              }
              case _ => throw new UnsupportedOperationException("Couldn't find ctor")
            }
          }
          case _ => throw new UnsupportedOperationException("Couldn't find class")
        }

        case (dd: DefDef) :: rest => env.get(dd.symbol) match {
          case Some(Extractors.Closure(evalEnv, params, body)) => {
            for (p <- params)
              evalEnv(p.symbol) = NonReducible(ReductionError.Synthetic, EmptyTree)
            build(evalEnv, body :: rest)
          }
          case _ => throw new UnsupportedOperationException("Couldn't find def")
        }

        case (ld: LabelDef) :: rest => env.get(ld.symbol) match {
          case Some(Extractors.Closure(evalEnv, params, body)) => {
            for (p <- params)
              evalEnv(p.symbol) = NonReducible(ReductionError.Synthetic, EmptyTree)
            build(evalEnv, body :: rest)
          }
          case _ => throw new UnsupportedOperationException("Couldn't find def")
        }

        case (fun: Function) :: rest => {
          val evalEnv = env.makeChild setSymbol fun.symbol
          for (p <- fun.vparams)
            evalEnv(p.symbol) = NonReducible(ReductionError.Synthetic, EmptyTree)

          build(evalEnv, fun.body :: rest)
        }

        case (blk: Block) :: Nil => env

        case (blk: Block) :: next :: rest => {

          val orderedStats = blk.stats.zipWithIndex sortBy {
            case (_: ValDef, index)    => (1, index)
            case (_: MemberDef, index) => (0, index)
            case (_, index)            => (1, index)
          } map { _._1 }

          val (pre, others) = blk.stats.partition(_ ne next)

          val post = others flatMap {
            case stat: ValDef    => Some(stat.copy(rhs = EmptyTree))
            case stat: MemberDef => Some(stat)
            case _               => None
          }

          val blkEnv = reduce(Block(pre ++ post, CurrentEnvironment), env, false) match {
            case env: Environment => env
            case _                => throw new UnsupportedOperationException("Error building block environment")
          }

          build(blkEnv, next :: rest)
        }

        case _ :: rest => build(env, rest)
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