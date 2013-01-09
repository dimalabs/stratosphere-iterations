package eu.stratosphere.pact4s.common.analysis.postPass

import scala.collection.mutable
import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.plan._

object AmbientFieldDetector {

  import Extractors._
  import EdgeDependencySets.EdgeDependencySet

  def updateAmbientFields(plan: OptimizedPlan, edgeDependencies: Map[PactConnection, EdgeDependencySet], outputPositions: Map[Int, GlobalPos]): Unit = {
    plan.getDataSinks().foldLeft(Set[OptimizerNode]())(updateAmbientFields(outputPositions, edgeDependencies))
  }

  private def updateAmbientFields(outputPositions: Map[Int, GlobalPos], edgeDependencies: Map[PactConnection, EdgeDependencySet])(visited: Set[OptimizerNode], node: OptimizerNode): Set[OptimizerNode] = {

    visited.contains(node) match {

      case true => visited

      case false => {
        node match {

          case _: SinkJoiner | _: UnionNode | _: CombinerNode =>
          case DataSinkNode(udf, input)                       =>
          case DataSourceNode(udf)                            =>

          case CoGroupNode(udf, _, _, leftInput, rightInput) => {

            val leftProvides = edgeDependencies(leftInput).childProvides
            val rightProvides = edgeDependencies(rightInput).childProvides
            val parentNeeds = edgeDependencies(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
          }

          case CrossNode(udf, leftInput, rightInput) => {

            val leftProvides = edgeDependencies(leftInput).childProvides
            val rightProvides = edgeDependencies(rightInput).childProvides
            val parentNeeds = edgeDependencies(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
          }

          case JoinNode(udf, _, _, leftInput, rightInput) => {

            val leftProvides = edgeDependencies(leftInput).childProvides
            val rightProvides = edgeDependencies(rightInput).childProvides
            val parentNeeds = edgeDependencies(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
          }

          case MapNode(udf, input) => {

            val inputProvides = edgeDependencies(input).childProvides
            val parentNeeds = edgeDependencies(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.forwardSet, udf.discardSet, inputProvides, parentNeeds, writes, outputPositions)
          }

          case ReduceNode(udf, _, input) => {
            val inputProvides = edgeDependencies(input).childProvides
            val parentNeeds = edgeDependencies(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.forwardSet, udf.discardSet, inputProvides, parentNeeds, writes, outputPositions)
          }
        }

        node.getIncomingConnections.map(_.getSourcePact).foldLeft(visited + node)(updateAmbientFields(outputPositions, edgeDependencies))
      }
    }
  }

  private def populateSets(forwards: mutable.Set[GlobalPos], discards: mutable.Set[GlobalPos], childProvides: Set[Int], parentNeeds: Set[Int], writes: Set[Int], outputPositions: Map[Int, GlobalPos]): Unit = {
    forwards.clear()
    forwards.addAll((parentNeeds -- writes).intersect(childProvides).map(outputPositions(_)))

    discards.clear()
    discards.addAll((childProvides -- parentNeeds -- writes).intersect(childProvides).map(outputPositions(_)))
  }
}

