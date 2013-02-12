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

package eu.stratosphere.pact4s.common.analysis.postPass

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.plan._

trait GlobalSchemaOptimizer {

  import Extractors._

  def optimizeSchema(plan: OptimizedPlan, compactSchema: Boolean): Unit = {

    val (outputSets, outputPositions) = OutputSets.computeOutputSets(plan)
    val edgeSchemas = EdgeDependencySets.computeEdgeDependencySets(plan, outputSets)

    AmbientFieldDetector.updateAmbientFields(plan, edgeSchemas, outputPositions)

    if (compactSchema) {
      GlobalSchemaCompactor.compactSchema(plan)
    }

    GlobalSchemaPrinter.printSchema(plan)
    
    plan.getDataSinks().foldLeft(Set[OptimizerNode]())(persistConfiguration)
  }

  private def persistConfiguration(visited: Set[OptimizerNode], node: OptimizerNode): Set[OptimizerNode] = {

    visited.contains(node) match {

      case true => visited

      case false => {

        val children = node.getIncomingConnections.map(_.getSourcePact).toSet
        val newVisited = children.foldLeft(visited + node)(persistConfiguration)

        node.getPactContract match {

          case c: Pact4sContract[_] => c.persistConfiguration(c.getParameters.getClassLoader)
          case _                    =>
        }

        newVisited
      }
    }
  }
}
