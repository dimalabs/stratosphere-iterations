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

import eu.stratosphere.pact4s.compiler.util.MutableMultiMap

trait SanityCheckers { this: Pact4sPlugin =>

  import global._
  import defs._

  trait SanityChecker extends Pact4sComponent {

    private val errors = new MutableMultiMap[CompilationUnit, Tree]()

    override def newTransformer(unit: CompilationUnit) = new TypingTraverser(unit) with Logger with TreeGenerator {

      override def apply(tree: Tree) = tree match {
        case Unanalyzed(tpe) => Error.report("Found " + tpe); errors(unit) += tree
        case _               => super.apply(tree)
      }

      private object Unanalyzed {
        def unapply(tree: Tree): Option[Type] = tree match {
          case Apply(fun, _) if unanalyzed.contains(fun.symbol) => Some(fun.tpe)
          case _ => None
        }
      }
    }

    override def afterRun() = {

      if (!errors.isEmpty && logger.level >= LogLevel.Inspect) {

        val units = errors.toList map {
          case (unit, trees) => {
            val errUnit = new CompilationUnit(unit.source)
            errUnit.body = trees.toList match {
              case List(body) => body
              case stats      => Block(stats, Literal(()))
            }
            errUnit
          }
        }

        treeBrowser.browse(phaseName + " Errors", units)
      }

      super.afterRun()
    }
  }
}