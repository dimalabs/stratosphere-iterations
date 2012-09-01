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

package eu.stratosphere.pact4s.compiler.util

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.TypingTransformers

trait Loggers { this: TypingTransformers =>

  val global: Global
  import global._

  abstract sealed class LogLevel { protected[Loggers] val toInt: Int }
  object LogLevel {
    case object Error extends LogLevel { override val toInt = 1 }
    case object Warn extends LogLevel { override val toInt = 2 }
    case object Debug extends LogLevel { override val toInt = 3 }
    case object Inspect extends LogLevel { override val toInt = 4 }
  }

  val logLevel: LogLevel = LogLevel.Debug
  private val counter = new Counter

  type LoggingTransformer = Logger#LoggingTransformer

  trait Logger { this: PluginComponent =>

    trait LoggingTransformer { this: TypingTransformer =>

      abstract sealed class Severity {
        protected val toInt: Int
        protected def reportInner(msg: String, pos: Position)

        def isEnabled = this.toInt <= logLevel.toInt
        private def curPos = if (curTree == null) NoPosition else curTree.pos

        def report(msg: String, pos: Position = curPos) = {
          if (isEnabled) {
            reportInner("%04d".format(counter.next) + "#" + phaseName + " - " + msg, pos)
          }
        }
      }

      case object Error extends Severity {
        override val toInt = LogLevel.Error.toInt
        override def reportInner(msg: String, pos: Position) = reporter.error(pos, msg)
      }

      case object Warn extends Severity {
        override val toInt = LogLevel.Warn.toInt
        override def reportInner(msg: String, pos: Position) = reporter.warning(pos, msg)
      }

      case object Debug extends Severity {
        override val toInt = LogLevel.Debug.toInt
        override def reportInner(msg: String, pos: Position) = reporter.info(pos, msg, true)
      }

      case object Inspect extends Severity {
        override val toInt = LogLevel.Inspect.toInt
        override def reportInner(msg: String, pos: Position) = reporter.info(pos, msg, true)

        def browse(tree: Tree) = {
          if (isEnabled)
            treeBrowsers.create().browse(tree)
        }
      }

      def getMsgAndStackLine(e: Throwable) = {
        val lines = e.getStackTrace.map(_.toString)
        val relevant = lines filter { _.contains("eu.stratosphere") }
        val stackLine = relevant.headOption getOrElse e.getStackTrace.toString
        e.getMessage() + " @ " + stackLine
      }

      def posString(pos: Position): String = pos match {
        case NoPosition => "?:?"
        case _          => pos.line + ":" + pos.column
      }

      def safely[T](default: => T)(onError: Throwable => String)(block: => T): T = {
        try {
          block
        } catch {
          case e => { Error.report(onError(e)); default }
        }
      }

      def verbosely[T](obs: T => String)(block: => T): T = {
        val ret = block
        Debug.report(obs(ret))
        ret
      }

      def maybeVerbosely[T](gate: Gate[T])(obs: T => String)(block: => T): T = {
        val ret = block
        if (gate.getState(ret)) Debug.report(obs(ret))
        ret
      }

      def visually(gate: Gate[Tree])(block: => Tree): Tree = {
        val ret = block
        if (gate.getState(ret)) Inspect.browse(ret)
        ret
      }

      trait Gate[T] {
        def getState(value: T): Boolean
      }

      class ManualSwitch[T] extends Gate[T] {
        var state = false;
        def getState(value: T) = state

        def |=(value: Boolean): Unit = state |= value
        def &=(value: Boolean): Unit = state &= value
      }

      class AutoSwitch[T] extends Gate[T] {

        protected var state = true

        def guard: Boolean = true

        def getState(value: T) = {
          val current = state && guard
          if (current) state = false
          current
        }
      }

      class EagerAutoSwitch[T] {

        protected var state = true
        def guard: Boolean = true

        def toGate: Gate[T] = {
          if (state && guard) {
            state = false
            new AutoSwitch[T]
          } else {
            new Gate[T] { def getState(value: T) = false }
          }
        }
      }

      object EagerAutoSwitch {
        implicit def toGate[T](eas: EagerAutoSwitch[T]): Gate[T] = eas.toGate
      }

      class SetGate[T] extends Gate[T] {
        private val seen = mutable.Set[T]()
        def getState(value: T) = seen.add(value)
      }

      class MapGate[K, V] {
        private val seen = mutable.Set[(K, V)]()
        def apply(key: K) = new Gate[V] {
          def getState(value: V) = seen.add(key, value)
        }
      }
    }
  }
}