package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global

trait Logger {

  val global: Global
  import global._

  def messageTag: String = ""
  def currentLevel: Severity = Severity.Debug
  def currentPosition: Position = NoPosition

  abstract sealed class Severity {
    protected val toInt: Int
    protected def reportInner(pos: Position, msg: String)

    final def report(pos: Position, msg: String) = {
      if (isEnabled) {
        reportInner(pos, messageTag + "#" + Logger.counter + " - " + msg)
        Logger.counter += 1
      }
    }

    final def browse(tree: Tree): Tree = {
      if (isEnabled)
        treeBrowsers.create().browse(tree)
      tree
    }

    final def isEnabled = currentLevel.toInt >= this.toInt
  }

  object Severity {
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
  }

  def log(severity: Severity, pos: Position = currentPosition)(msg: String) = severity.report(pos, msg)

  def safely[T](default: => T)(onError: Throwable => String)(block: => T): T = {
    try {
      block
    } catch {
      case e => { log(Severity.Error)(onError(e)); default }
    }
  }

  def verbosely[T](obs: T => String)(block: => T): T = {
    val ret = block
    log(Severity.Debug)(obs(ret))
    ret
  }

  def visually(gate: Boolean)(block: => Tree): Tree = {
    val ret = block
    if (gate) Severity.Debug.browse(ret)
    ret
  }

  class Switch {

    private var state = true

    def guard: Boolean = true

    final def getState = {
      val current = state && guard
      if (current) state = false
      current
    }
  }

  object Switch {
    implicit def toBoolean(switch: Switch): Boolean = switch.getState
  }
}

object Logger {
  private var counter = 100;
}

