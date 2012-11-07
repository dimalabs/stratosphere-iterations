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

package eu.stratosphere.pact4s.compiler.util.treeReducers

import eu.stratosphere.pact4s.compiler.util._

/**
 * Warning: experimental!
 */
trait TreeReducers extends TreeReducerImpls with EnvironmentBuilderImpls { this: HasGlobal with TreeGenerators with Loggers =>

  object TreeReducerHelpers extends InheritsGlobal with Environments with NonReducibles with Closures with SymbolFactories

  trait TreeReducer extends InheritsGlobal with TreeReducerImpl with EnvironmentBuilders { this: TreeGenerator with Logger =>

    import global._
    import TreeReducerHelpers.Environment

    object treeReducer {

      def reduce(units: List[CompilationUnit], strictBlocks: Boolean): Environment = {
        val env = Environment.Empty.makeChild
        units foreach { unit => reduce(unit.body, env, strictBlocks) }
        env
      }

      def reduce(tree: Tree, env: Environment, strictBlocks: Boolean): Tree = (new DeterministicTreeReducer(strictBlocks, tree)).reduce(tree, env)

      def build(env: Environment, path: List[Tree]): Environment = (new EnvironmentBuilder).build(env, path)
    }
  }
}