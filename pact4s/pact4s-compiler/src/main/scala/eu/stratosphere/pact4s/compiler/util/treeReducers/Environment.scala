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

import scala.collection.mutable
import eu.stratosphere.pact4s.compiler.util._

trait TreeReducerEnvironments { this: TreeReducers with HasGlobal =>

  import global._
  import TreeReducer.NonReducible
  import TreeReducer.ReductionError

  trait TreeReducerEnvironment {

    object Environment {

      def apply(): Environment = Empty.makeChild

      object Empty extends Environment {
        override val parent = null
        override def symbol_=(sym: Symbol) = throw new UnsupportedOperationException("Attempted to set symbol on non-existent environment")
        override def findParent(pred: Environment => Boolean) = None
        override def get(sym: Symbol) = None
        override def apply(sym: Symbol) = throw new UnsupportedOperationException("Attempted to retrieve value from non-existent environment")
        override def update(sym: Symbol, value: Tree) = throw new UnsupportedOperationException("Attempted to modify non-existent environment")
        override def copy(cache: mutable.Map[Environment, Environment]): Environment = this
      }

      protected[Environment] class Member(val symbol: Symbol, private var _value: Tree, initialized: Boolean) {

        private var _initialized: Boolean = initialized
        private var _accessed: Boolean = false
        private var _mutated: Boolean = false

        def value = { _accessed = true; _value }
        def value_=(value: Tree) = {

          if (_initialized)
            _mutated = true
          else
            _initialized = true

          _value = value
        }

        def accessed = _accessed
        def mutated = _mutated

        def valueMutated = _value match {
          case env: Environment => env.mutated
          case _                => false
        }

        def panic: Unit = {
          _accessed = true

          _value match {
            case env: Environment => env.panic
            case _                =>
          }

          if (symbol.isMutable) {
            _initialized = true
            _mutated = true
            _value = TreeReducer.NonReducible(ReductionError.NoSource, _value)
          }
        }

        def copy(cache: mutable.Map[Environment, Environment]): Member = {
          val value = _value match {
            case env: Environment => env.copy(cache)
            case x                => x
          }
          new Member(symbol, value, _initialized)
        }

        def merge(that: Member): Unit = {

          val changed = (_initialized != that._initialized) || that._mutated

          _accessed |= that._accessed
          _initialized |= changed
          _mutated |= changed

          (changed, _value, that._value) match {
            case (true, _, _)                                    => _value = TreeReducer.NonReducible(ReductionError.NonDeterministic, _value)
            case (false, env: Environment, thatEnv: Environment) => env.merge(thatEnv)
            case _                                               =>
          }
        }
      }
    }

    abstract class Environment extends Tree {

      import Environment.Member

      protected val parent: Environment
      protected val members: mutable.Map[Symbol, Member] = mutable.Map()
      private val panicking = new util.DynamicVariable(false)

      private var _symbol: Symbol = NoSymbol
      override def hasSymbol = true
      override def symbol = _symbol
      override def symbol_=(sym: Symbol) = _symbol = sym

      override def productArity = 1
      override def productElement(n: Int): Any = n match {
        case 0 => parent
        case _ => throw new IndexOutOfBoundsException
      }
      override def canEqual(that: Any): Boolean = that match {
        case _: Environment => true
        case _              => false
      }

      def findParent(pred: Environment => Boolean): Option[Environment] = pred(this) match {
        case true  => Some(this)
        case false => parent.findParent(pred)
      }

      def find(sym: Symbol): Option[Tree] = findParent(_.defines(sym)) match {
        case Some(env) => env get sym
        case None => sym.owner match {
          case null | NoSymbol => None
          case owner => find(owner) match {
            case Some(env: Environment) => env get sym
            case _                      => None
          }
        }
      }

      def get(sym: Symbol): Option[Tree] = members get sym map { m => m.value }

      def apply(sym: Symbol): Tree = get(sym).get

      def update(sym: Symbol, value: Tree): Unit = members get sym match {
        case Some(member) => member.value = value
        case None         => members(sym) = new Member(sym, value, true)
      }

      def initMember(sym: Symbol, default: Tree): Unit = members get sym match {
        case Some(_) =>
        case None    => members(sym) = new Member(sym, default, false)
      }

      def defines(sym: Symbol): Boolean = members.contains(sym)

      def mutated: Boolean = members exists { case (_, m) => m.mutated || m.valueMutated }

      def makeChild: Environment = {
        val self = this
        new Environment { override val parent = self }
      }

      def asChildOf(that: Environment): Environment = {

        abstract class EnvProxy extends Environment {
          protected val src: Environment

          override def copy(cache: mutable.Map[Environment, Environment]) = {
            val self = this
            val that = new EnvProxy {
              cache(self) = this
              override val src = self.src.copy(cache)
              override val parent = self.parent.copy(cache)
              override val members = src.members
            }

            that setSymbol self.symbol
          }
        }

        val self = this
        new EnvProxy {
          override val src = self
          override val parent = that
          override val members = self.members
        }
      }

      def panic: Unit = {
        if (!panicking.value) {
          panicking.withValue(true) {
            for (member <- members.values) {
              member.panic
            }
            parent.panic
          }
        }
      }

      def copy: Environment = copy(mutable.Map())

      protected def copy(cache: mutable.Map[Environment, Environment]): Environment = cache get this getOrElse {
        val self = this
        val that = new Environment {
          cache(self) = this
          override val parent = self.parent.copy(cache)
        }

        for ((sym, member) <- members) {
          that.members(sym) = member.copy(cache)
        }

        that setSymbol self.symbol
      }

      def merge(that: Environment): Unit = {
        for ((sym, member) <- members) {
          member.merge(that.members(sym))
        }
      }
    }
  }
}