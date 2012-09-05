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

import scala.tools.nsc.Phase
import scala.tools.nsc.plugins.PluginComponent

trait Transform extends PluginComponent {

  import global._

  protected def newTransformer(unit: global.CompilationUnit): global.Transformer

  protected def afterRun() = {}

  def newPhase(prev: Phase): StdPhase = new StdPhase(prev) {

    override def run() = {
      super.run()
      afterRun()
    }

    def apply(unit: global.CompilationUnit) {
      newTransformer(unit).transformUnit(unit)
    }
  }
}
