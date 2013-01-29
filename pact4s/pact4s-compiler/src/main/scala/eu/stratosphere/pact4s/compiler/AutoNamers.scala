/**
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
 */

package eu.stratosphere.pact4s.compiler

import scala.util.DynamicVariable

trait AutoNamers { this: Pact4sPlugin =>

  import global._
  import defs._

  trait AutoNamer extends Pact4sComponent {

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with Logger with TreeGenerator {

      private val currentName = new DynamicVariable("")

      override def apply(tree: Tree) = tree match {

        case ValDef(_, name, _, _)       => currentName.withValue(name.toString.trim()) { super.apply(tree) }
        case DefDef(_, name, _, _, _, _) => currentName.withValue(name.toString.trim()) { super.apply(tree) }

        case Assign(lhs, rhs) => {
          val newLhs = super.apply(lhs)
          val newRhs = currentName.withValue(lhs.symbol.name.toString.trim()) { super.apply(rhs) }
          treeCopy.Assign(tree, newLhs, newRhs)
        }

        case _: Apply if canAutoName(tree.tpe) => safely(tree, false)(err => "Error applying name " + currentName.value + " to expression " + tree.toString) {
          localTyper.typed {
            val fun = mkSelect("eu", "stratosphere", "pact4s", "common", "Hintable", "withNameIfNotSet") setPos tree.pos
            val tFun = TypeApply(fun, List(TypeTree(tree.tpe) setPos tree.pos)) setPos tree.pos
            val nameArg = Literal(currentName.value) setPos tree.pos
            Apply(fun, List(super.apply(tree), nameArg)) setPos tree.pos
          }
        }

        case _ => super.apply(tree)
      }

      private def canAutoName(tpe: Type): Boolean = (currentName.value.length > 0) && (tpe.baseClasses.contains(hintableClass))
    }
  }
}
