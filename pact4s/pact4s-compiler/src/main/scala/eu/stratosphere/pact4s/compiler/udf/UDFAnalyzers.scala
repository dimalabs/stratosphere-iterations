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

trait UDFAnalyzers extends Unlifters with SelectorAnalyzers with FlowAnalyzers with FallbackBinders { this: Pact4sPlugin =>

  import global._
  import defs._

  trait UDFAnalyzer extends Pact4sComponent {

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with TreeGenerator with Logger with Unlifter with SelectorAnalyzer with FlowAnalyzer with FallbackBinder {

      val trans = (unlift _) andThen analyzeSelector andThen analyzeFlow andThen bindDefaultUDF

      override def apply(tree: Tree) = super.apply { trans(tree) }

    }
  }
}

