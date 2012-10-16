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

trait SymbolFactories { this: HasGlobal with Environments =>

  import global._

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
}