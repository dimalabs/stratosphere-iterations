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

trait Logger {

  val global: Global
  import global._

  val logger = new {
    var messageTag: String = ""
    var currentLevel: Severity = Debug
    var currentPosition: Position = NoPosition
  }

  import logger._

  private val counter = new Counter

  abstract sealed class Severity {
    protected val toInt: Int
    protected def reportInner(pos: Position, msg: String)

    def isEnabled = currentLevel.toInt >= this.toInt

    def report(msg: String, pos: Position = currentPosition) = {
      if (isEnabled)
        reportInner(pos, "%04d".format(counter.next) + "#" + messageTag + " - " + msg)
    }

    def browse(tree: Tree): Tree = {
      if (isEnabled)
        treeBrowsers.create().browse(tree)
      tree
    }
  }

  case object Error extends Severity {
    override val toInt = 1
    override def reportInner(pos: Position, msg: String) = reporter.error(pos, msg)
  }

  case object Warn extends Severity {
    override val toInt = 2
    override def reportInner(pos: Position, msg: String) = reporter.warning(pos, msg)
  }

  case object Debug extends Severity {
    override val toInt = 3
    override def reportInner(pos: Position, msg: String) = reporter.info(pos, msg, true)
  }

  def log(severity: Severity, pos: Position = currentPosition)(msg: String) = severity.report(msg, pos)

  def getMsgAndStackLine(e: Throwable) = {
    val lines = e.getStackTrace.map(_.toString)
    val relevant = lines filter { _.contains("eu.stratosphere") }
    val stackLine = relevant.headOption getOrElse e.getStackTrace.toString
    e.getMessage() + " @ " + stackLine
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
    if (gate.getState(ret)) Debug.browse(ret)
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

