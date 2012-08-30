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

import scala.tools.nsc.Global
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

trait TypingTraversers { this: TypingTransformers =>

  import global._

  trait Traverser extends Transformer {

    private var path = Seq[Tree]()
    def currentPath = path

    def traverse(tree: Tree): Unit = {
      super.transform(tree)
    }

    def isPathComponent(tree: Tree): Boolean = true

    override def transform(tree: Tree): Tree = {

      val isPathComp = isPathComponent(tree)

      if (isPathComp)
        path = tree +: path

      traverse(tree)

      if (isPathComp)
        path = path.tail

      tree
    }
  }

  class TypingTraverser(unit: CompilationUnit) extends TypingTransformer(unit) with Traverser
}