package eu.stratosphere.pact4s.common.analysis.postPass

import scala.collection.JavaConversions._
import scala.collection.mutable

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.postpass.OptimizerPostPass
import eu.stratosphere.pact.compiler.plan._

trait GlobalSchemaOptimizer extends OptimizerPostPass {

  import Extractors._
  import EdgeFieldSets.EdgeFieldSet

  override def postPass(plan: OptimizedPlan): Unit = {

    val (outputSets, outputPositions) = OutputSets.computeOutputSets(plan)
    val edgeFieldSets = EdgeFieldSets.computeEdgeFieldSets(plan, outputSets)

    plan.getDataSinks().foldLeft(Set[OptimizerNode]())(computeAmbientFieldSets(outputPositions, edgeFieldSets))

    GlobalSchemaPrinter.printSchema(plan)
  }

  private def computeAmbientFieldSets(outputPositions: Map[Int, GlobalPos], edgeFieldSets: Map[PactConnection, EdgeFieldSet])(visited: Set[OptimizerNode], node: OptimizerNode): Set[OptimizerNode] = {

    visited.contains(node) match {

      case true => visited

      case false => {
        node match {

          case _: SinkJoiner | _: UnionNode | _: CombinerNode =>
          case DataSinkNode(udf, input)                       =>
          case DataSourceNode(udf)                            =>

          case CoGroupNode(udf, _, _, leftInput, rightInput) => {

            val leftProvides = edgeFieldSets(leftInput).childProvides
            val rightProvides = edgeFieldSets(rightInput).childProvides
            val parentNeeds = edgeFieldSets(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
          }

          case CrossNode(udf, leftInput, rightInput) => {

            val leftProvides = edgeFieldSets(leftInput).childProvides
            val rightProvides = edgeFieldSets(rightInput).childProvides
            val parentNeeds = edgeFieldSets(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
          }

          case JoinNode(udf, _, _, leftInput, rightInput) => {

            val leftProvides = edgeFieldSets(leftInput).childProvides
            val rightProvides = edgeFieldSets(rightInput).childProvides
            val parentNeeds = edgeFieldSets(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
          }

          case MapNode(udf, input) => {

            val inputProvides = edgeFieldSets(input).childProvides
            val parentNeeds = edgeFieldSets(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.forwardSet, udf.discardSet, inputProvides, parentNeeds, writes, outputPositions)
          }

          case ReduceNode(udf, _, input) => {
            val inputProvides = edgeFieldSets(input).childProvides
            val parentNeeds = edgeFieldSets(node.getOutConns.head).childProvides
            val writes = udf.outputFields.toIndexSet

            populateSets(udf.forwardSet, udf.discardSet, inputProvides, parentNeeds, writes, outputPositions)
          }
        }

        node.getIncomingConnections.map(_.getSourcePact).foldLeft(visited + node)(computeAmbientFieldSets(outputPositions, edgeFieldSets))
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
