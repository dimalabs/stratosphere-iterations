package eu.stratosphere.pact4s.compiler.util

import scala.collection.mutable

trait MemoizedPartialFunction[A, B] extends PartialFunction[A, B] {

  protected def update(input: A, result: B): Unit
  def undefine(input: A): Unit

  // Syntactic sugar for conditional updates:
  // fun.iffResult { y => pred(y) } {
  //   fun.define(x) = y
  // }

  def iff(pred: (A, B) => Boolean)(elem: ElementDefinition) = {
    val input = elem.input

    if (!elem.redefine && isDefinedAt(input)) {
      apply(input)
    } else {
      val result = elem.getResult()
      if (pred(input, result))
        this(input) = result
      result
    }
  }

  def iffInput(pred: A => Boolean) = iff { case (input, _) => pred(input) } _
  def iffResult(pred: B => Boolean) = iff { case (_, result) => pred(result) } _

  def unless(pred: (A, B) => Boolean) = iff { !pred(_, _) } _
  def unlessInput(pred: A => Boolean) = iffInput { !pred(_) }
  def unlessResult(pred: B => Boolean) = iffResult { !pred(_) }

  object define {
    def update(input: A, result: => B): ElementDefinition = new ElementDefinition(false, input, { _ => result })
  }

  object redefine {
    def update(input: A, result: Option[B] => B): ElementDefinition = new ElementDefinition(true, input, { _ => result(lift.apply(input)) })
  }

  class ElementDefinition(val redefine: Boolean, val input: A, result: Unit => B) {
    def getResult = result
  }

  object ElementDefinition {

    // Syntactic sugar for unconditional updates: 
    // fun.define(x) = y
    implicit def toValue(elem: ElementDefinition): B = {
      if (!elem.redefine && isDefinedAt(elem.input)) {
        apply(elem.input)
      } else {
        val result = elem.getResult()
        MemoizedPartialFunction.this(elem.input) = result
        result
      }
    }
  }

  def liftWithDefault(default: A => B) = { input: A =>
    if (this.isDefinedAt(input))
      this(input)
    else
      default(input)
  }

  def liftWithDefaultDefine(default: A => B) = { input: A =>
    if (this.isDefinedAt(input)) {
      this(input)
    } else {
      val value = default(input)
      this(input) = value
      value
    }
  }
}

class MapMemoizedPartialFunction[A, B] extends MemoizedPartialFunction[A, B] {

  private val cache = mutable.Map[A, B]()

  override def isDefinedAt(input: A): Boolean = cache.isDefinedAt(input)
  override def apply(input: A): B = cache(input)
  override def update(input: A, result: B): Unit = cache(input) = result
  override def undefine(input: A): Unit = cache.remove(input)
}

object MemoizedPartialFunction {

  def apply[A, B](): MemoizedPartialFunction[A, B] = new MapMemoizedPartialFunction[A, B]()
}
