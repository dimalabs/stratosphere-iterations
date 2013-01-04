package eu.stratosphere.pact4s.common.analysis

import scala.collection.mutable
import scala.reflect.Code

/**
 * Instances of this class are typically created by the Pact4s compiler plugin. The plugin detects
 * call sites that require converting a lambda literal to a FieldSelector, analyzes the lambda
 * argument to determine which fields are selected, and injects a call to this class's constructor.
 *
 * In addition to the language restrictions applied to the lambda expression, the selected fields
 * must also be top-level. Nested fields (such as a list element or an inner instance of a
 * recursive type) are not allowed.
 *
 * @param T The type In => Key of the selector function
 * @param udtIn The UDT[In] instance
 * @param selection The selected fields
 */
class FieldSelector[T <: Function1[_, _]](udtIn: UDT[_], selection: List[Int]) extends FieldSelector.EmptyCode[T] with Serializable {
  
  /**
   * Selects T.* via UDT[T].getFieldIndexes(Seq(Seq()))
   */
  def this(udtIn: UDT[_]) = this(udtIn, udtIn.getFieldIndexes(Seq(Seq())))

  def this(udtIn: UDT[_], selection: Seq[Seq[String]]) = this(udtIn, udtIn.getFieldIndexes(selection))

  val inputFields = FieldSet.newInputSet(udtIn.numFields)
  val selectedFields = inputFields.select(selection)

  for (field <- inputFields.diff(selectedFields))
    field.isUsed = false
}

/**
 * A key selector is a field selector with the added constraint that all selected fields must be
 * valid PACT keys (no lists, recursive types, etc.).
 *
 * @param T The type In => Key of the selector function
 * @param udtIn The UDT[In] instance
 * @param selection The selected fields
 */
class KeySelector[T <: Function1[_, _]](udtIn: UDT[_], selection: List[Int]) extends FieldSelector[T](udtIn, selection) {

  /**
   * Selects T.* via UDT[T].getFieldIndexes(Seq(Seq()))
   */
  def this(udtIn: UDT[_]) = this(udtIn, udtIn.getFieldIndexes(Seq(Seq())))

  def this(udtIn: UDT[_], selection: Seq[Seq[String]]) = this(udtIn, udtIn.getFieldIndexes(selection))
}

/**
 * The following implicits are beacons to trigger the Pact4s compiler plugin's conversion of lambda
 * literals to selectors. There is a bit of scala compiler black magic involved here:
 *
 * Assume we have the following function: class Op[T] { def op[R](userFun: FieldSelector[T => R]) = ... }
 * And the call site: obj.op({ x => ... })
 *
 * Because the function definition requires a parameter of type FieldSelector[T => R] and not T => R,
 * type inference will fail for the parameter x of the lambda expression at the call site. But if the
 * definition's parameter type inherits from Code[T => R], then the argument at the call site will be 
 * type-checked as the function type T => R, thereby allowing type inference to succeed. Note that this 
 * works for FieldSelector[T => R] but not FieldSelector[T, R] (the first generic parameter must be the 
 * function type).
 *
 * This comes with a drawback, however. Code[T => R] is the user-facing hook for scala's experimental
 * code lifting algorithm. To coerce a value of type T => R to type Code[T => R], the scala type checker
 * injects a call to the implicit Code.lift method, so the call site becomes:
 *
 * obj.op(FieldSelector.unanalyzedFieldSelectorCode(Code.lift({ x => ... })))
 *
 * When the lifter phase encounters a call to Code.lift, it will attempt to lift the argument expression
 * to a runtime AST. This is problematic because the lifting engine is extremely limited (and because we
 * don't actually need an AST representation). To enable correct type inference behavior without triggering
 * the code lifting algorithm, the Pact4s compiler plugin will detect and rewrite call sites of the form:
 *
 * [Field|Key]Selector.unanalyzed[Field|Key]SelectorCode(Code.lift({ x => ... }))
 *
 * Stripping the inner Code.lift application and rebinding the outer application yields the call site:
 *
 * obj.op(FieldSelector.unanalyzedFieldSelector({ x => ... }))
 *
 * After analyzing the user function, this call site is then rewritten to:
 *
 * obj.op(new FieldSelector(Seq(Seq("f1_1", ..., "f1_n"), ..., Seq("fn_1", ..., "fn_n")))
 */

trait FieldSelectorLowPriorityImplicits {

  class FieldSelectorAnalysisFailedException extends RuntimeException("Field selector analysis failed. This should have been caught at compile time.")

  implicit def unanalyzedFieldSelector[T1: UDT, R](fun: T1 => R): FieldSelector[T1 => R] = throw new FieldSelectorAnalysisFailedException
  implicit def unanalyzedFieldSelectorCode[T1: UDT, R](fun: Code[T1 => R]): FieldSelector[T1 => R] = throw new FieldSelectorAnalysisFailedException
}

object FieldSelector extends FieldSelectorLowPriorityImplicits {
  // This class is necessary to support Java serialization: Code[T] does not have a no-arg constructor, while EmptyCode[T] does.
  // Java serialization is required because a FieldSelector instance might get caught up in the closure of a user code function.
  protected abstract class EmptyCode[T] extends Code[T](null) {
    @transient override val tree = null
  }
}

trait KeySelectorLowPriorityImplicits {

  class KeySelectorAnalysisFailedException extends RuntimeException("Key selector analysis failed. This should have been caught at compile time.")

  implicit def unanalyzedKeySelector[T1: UDT, R](fun: T1 => R): KeySelector[T1 => R] = throw new KeySelectorAnalysisFailedException
  implicit def unanalyzedKeySelectorCode[T1: UDT, R](fun: Code[T1 => R]): KeySelector[T1 => R] = throw new KeySelectorAnalysisFailedException
}

object KeySelector extends KeySelectorLowPriorityImplicits

