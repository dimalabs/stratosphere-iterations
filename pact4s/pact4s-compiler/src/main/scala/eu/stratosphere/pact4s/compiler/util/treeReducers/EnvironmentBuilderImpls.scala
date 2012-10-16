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

trait EnvironmentBuilderImpls { this: HasGlobal with TreeReducers with TreeGenerators with Loggers with TreeReducerImpls =>

  import global._
  import TreeReducerHelpers._

  trait EnvironmentBuilders extends InheritsGlobal { this: TreeGenerator with Logger with TreeReducer =>

    case object CurrentEnvironment extends Tree

    class EnvironmentBuilder {

      def build(env: Environment, path: List[Tree]): Environment = path match {

        case Nil => env

        case (pd: PackageDef) :: rest => env.get(pd.symbol) match {
          case Some(env: Environment) => build(env, rest)
          case _                      => throw new UnsupportedOperationException("Couldn't find package")
        }

        case (cd: ClassDef) :: rest => env.get(cd.symbol) match {
          case Some(inst: Environment) if cd.symbol.isModuleClass => build(inst, rest)
          case Some(classEnv: Environment) => {

            val ctor = Select(New(TypeTree(cd.symbol.tpe)), cd.symbol.primaryConstructor)
            val newExpr = cd.symbol.primaryConstructor.paramss.foldLeft(ctor: Tree) { (fun, params) => Apply(fun, params map { _ => NonReducible(ReductionError.Synthetic, EmptyTree) }) }
            treeReducer.reduce(newExpr, env, false) match {
              case inst: Environment => build(inst, rest)
              case _                 => throw new UnsupportedOperationException("Couldn't construct instance")
            }

            /*
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
            */
          }
          case _ => throw new UnsupportedOperationException("Couldn't find class")
        }

        case (dd: DefDef) :: rest => env.get(dd.symbol) match {
          case Some(Closure(evalEnv, params, body)) => {
            for (p <- params)
              evalEnv(p.symbol) = NonReducible(ReductionError.Synthetic, EmptyTree)
            build(evalEnv, body :: rest)
          }
          case _ => throw new UnsupportedOperationException("Couldn't find def")
        }

        case (ld: LabelDef) :: rest => env.get(ld.symbol) match {
          case Some(Closure(evalEnv, params, body)) => {
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

          val blkEnv = treeReducer.reduce(Block(pre ++ post, CurrentEnvironment), env, false) match {
            case env: Environment => env
            case _                => throw new UnsupportedOperationException("Error building block environment")
          }

          build(blkEnv, next :: rest)
        }

        case _ :: rest => build(env, rest)
      }
    }
  }
}