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
import scala.concurrent.Lock

import scala.tools.nsc.plugins.PluginComponent

trait Loggers { this: TypingTransformers =>

  import global._
  import treeBrowsers.BrowserFrame
  import treeBrowsers.ASTTreeModel

  abstract sealed class LogLevel extends Ordered[LogLevel] {
    protected[Loggers] val toInt: Int
    override def compare(that: LogLevel) = this.toInt.compare(that.toInt)
  }

  object LogLevel {
    def unapply(name: String): Option[LogLevel] = name match {
      case "error" | "Error"     => Some(Error)
      case "warn" | "Warn"       => Some(Warn)
      case "debug" | "Debug"     => Some(Debug)
      case "inspect" | "Inspect" => Some(Inspect)
      case _                     => None
    }
    case object Error extends LogLevel { override val toInt = 1 }
    case object Warn extends LogLevel { override val toInt = 2 }
    case object Debug extends LogLevel { override val toInt = 3 }
    case object Inspect extends LogLevel { override val toInt = 4 }
  }

  object logger { var level: LogLevel = LogLevel.Warn }
  private val counter = new Counter

  trait Logger { this: TypingVisitor =>

    abstract sealed class Severity {
      protected val toInt: Int
      protected def reportInner(msg: String, pos: Position)

      protected def formatMsg(msg: String) = "%04d".format(counter.next) + "#" + globalPhase.name + " - " + msg

      def isEnabled = this.toInt <= logger.level.toInt

      def report(msg: String) = {
        if (isEnabled) {
          reportInner(formatMsg(msg), curPos)
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

      def report(msg: String, tree: Tree) = {
        if (isEnabled) {
          reporter.flush()
          val lock = new Lock()
          val frame = new BrowserFrame(formatMsg(msg))
          frame.setTreeModel(new ASTTreeModel(tree))
          frame.createFrame(lock)
          lock.acquire
        }
      }

      def report(msg: String, units: Seq[CompilationUnit]) = {
        if (isEnabled)
          treeBrowser.browse(formatMsg(msg), units.toIterator)
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

    def safely(default: => Tree, inspect: Boolean)(onError: Throwable => String)(block: => Tree): Tree = {
      try {
        block
      } catch {
        case e => {
          Error.report(onError(e));
          val ret = default
          if (inspect) Inspect.report("Error", ret)
          ret
        }
      }
    }

    def verbosely[T](obs: T => String)(block: => T): T = {
      val ret = block
      Debug.report(obs(ret))
      ret
    }

    def maybeVerbosely[T](guard: T => Boolean)(obs: T => String)(block: => T): T = {
      val ret = block
      if (guard(ret)) Debug.report(obs(ret))
      ret
    }
  }
}

