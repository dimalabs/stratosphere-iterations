package eu.stratosphere.pact4s.compiler.util

import scala.collection.mutable

import scala.tools.nsc.Global

trait Logger {

  val global: Global
  import global._

  private var counter = 0;

  var messageTag: String = ""
  var currentLevel: Severity = Debug
  var currentPosition: Position = NoPosition

  abstract sealed class Severity {
    protected val toInt: Int
    protected def reportInner(pos: Position, msg: String)

    def isEnabled = currentLevel.toInt >= this.toInt

    def report(msg: String, pos: Position = currentPosition) = {
      if (isEnabled) {
        reportInner(pos, "%03d".format(counter) + "#" + messageTag + " - " + msg)
        counter += 1
      }
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

