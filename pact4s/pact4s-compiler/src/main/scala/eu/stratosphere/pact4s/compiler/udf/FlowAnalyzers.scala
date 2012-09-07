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

package eu.stratosphere.pact4s.compiler.udf

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait FlowAnalyzers { this: Pact4sPlugin =>

  import global._
  import defs._

  trait FlowAnalyzer { this: TypingTransformer with TreeGenerator with Logger =>

    protected object AnalyzedUDF {

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, tpeTrees), List(fun)) if view.symbol == unanalyzedUDF1 => None
        case Apply(TypeApply(view, tpeTrees), List(fun)) if view.symbol == unanalyzedUDF2 => None
        case _ => None
      }
    }
  }
}
