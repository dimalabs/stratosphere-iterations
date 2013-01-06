package eu.stratosphere.pact4s.common.analysis.postPass

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.plan._

object EdgeFieldSets {

  import Extractors._

  case class EdgeFieldSet(parentNeeds: Set[Int], childProvides: Set[Int] = Set())

  def computeEdgeFieldSets(plan: OptimizedPlan, outputSets: Map[OptimizerNode, Set[Int]]): Map[PactConnection, EdgeFieldSet] = {
    
    plan.getDataSinks().foldLeft(Map[PactConnection, EdgeFieldSet]())(computeEdgeFieldSets(outputSets))
  }

  private def computeEdgeFieldSets(outputSets: Map[OptimizerNode, Set[Int]])(edgeFieldSets: Map[PactConnection, EdgeFieldSet], node: OptimizerNode): Map[PactConnection, EdgeFieldSet] = {

    // breadth-first traversal: parentNeeds will be None if any parent has not yet been visited
    val parentNeeds = node.getOutConns.foldLeft(Option(Set[Int]())) {
      case (None, _)           => None
      case (Some(acc), parent) => edgeFieldSets.get(parent) map { acc ++ _.parentNeeds }
    }

    parentNeeds match {
      case None                => edgeFieldSets
      case Some(parentNeeds) => computeEdgeFieldSets(node, parentNeeds, outputSets, edgeFieldSets)
    }
  }

  private def computeEdgeFieldSets(node: OptimizerNode, parentNeeds: Set[Int], outputSets: Map[OptimizerNode, Set[Int]], edgeFieldSets: Map[PactConnection, EdgeFieldSet]): Map[PactConnection, EdgeFieldSet] = {

    def updateEdges(needs: (PactConnection, Set[Int])*): Map[PactConnection, EdgeFieldSet] = {

      val updParents = node.getOutConns.foldLeft(edgeFieldSets) { (edgeFieldSets, parent) =>
        val entry = edgeFieldSets(parent)
        edgeFieldSets.updated(parent, entry.copy(childProvides = parentNeeds))
      }

      needs.foldLeft(updParents) {
        case (edgeFieldSets, (inConn, needs)) => {
          val updInConn = edgeFieldSets.updated(inConn, EdgeFieldSet(needs))
          computeEdgeFieldSets(outputSets)(updInConn, inConn.getSourcePact)
        }
      }
    }
    
    for (udf <- node.getUDF) {

      // suppress outputs that aren't needed by any parent
      val writeFields = udf.outputFields filter { _.isUsed }
      val unused = writeFields filterNot { f => parentNeeds.contains(f.globalPos.getValue) }
      
      for (field <- unused)
        field.isUsed = false
    }

    node match {

      case DataSinkNode(udf, input) => {
        val needs = udf.inputFields.toIndexSet
        updateEdges(input -> needs)
      }

      case DataSourceNode(udf) => {
        updateEdges()
      }

      case CoGroupNode(udf, leftKey, rightKey, leftInput, rightInput) => {

        val leftReads = udf.leftInputFields.toIndexSet ++ leftKey.selectedFields.toIndexSet
        val rightReads = udf.rightInputFields.toIndexSet ++ rightKey.selectedFields.toIndexSet
        val writes = udf.outputFields.toIndexSet

        val preNeeds = parentNeeds -- writes

        val leftOutputs = outputSets(leftInput.getSourcePact)
        val leftNeeds = preNeeds.intersect(leftOutputs) ++ leftReads

        val rightOutputs = outputSets(rightInput.getSourcePact)
        val rightNeeds = preNeeds.intersect(rightOutputs) ++ rightReads

        updateEdges(leftInput -> leftNeeds, rightInput -> rightNeeds)
      }

      case CrossNode(udf, leftInput, rightInput) => {

        val leftReads = udf.leftInputFields.toIndexSet
        val rightReads = udf.rightInputFields.toIndexSet
        val writes = udf.outputFields.toIndexSet

        val preNeeds = parentNeeds -- writes

        val leftOutputs = outputSets(leftInput.getSourcePact)
        val leftNeeds = preNeeds.intersect(leftOutputs) ++ leftReads

        val rightOutputs = outputSets(rightInput.getSourcePact)
        val rightNeeds = preNeeds.intersect(rightOutputs) ++ rightReads

        updateEdges(leftInput -> leftNeeds, rightInput -> rightNeeds)
      }

      case JoinNode(udf, leftKey, rightKey, leftInput, rightInput) => {

        val leftReads = udf.leftInputFields.toIndexSet ++ leftKey.selectedFields.toIndexSet
        val rightReads = udf.rightInputFields.toIndexSet ++ rightKey.selectedFields.toIndexSet
        val writes = udf.outputFields.toIndexSet

        val preNeeds = parentNeeds -- writes

        val leftOutputs = outputSets(leftInput.getSourcePact)
        val leftNeeds = preNeeds.intersect(leftOutputs) ++ leftReads

        val rightOutputs = outputSets(rightInput.getSourcePact)
        val rightNeeds = preNeeds.intersect(rightOutputs) ++ rightReads

        updateEdges(leftInput -> leftNeeds, rightInput -> rightNeeds)
      }

      case MapNode(udf, input) => {

        val reads = udf.inputFields.toIndexSet
        val writes = udf.outputFields.toIndexSet

        val needs = parentNeeds -- writes ++ reads

        updateEdges(input -> needs)
      }

      case ReduceNode(udf, key, input) => {

        val reads = udf.inputFields.toIndexSet ++ key.selectedFields.toIndexSet
        val writes = udf.outputFields.toIndexSet

        val needs = parentNeeds -- writes ++ reads

        updateEdges(input -> needs)
      }

      case _: SinkJoiner | _: UnionNode | _: CombinerNode => {
        updateEdges(node.getIncomingConnections.map(_ -> parentNeeds): _*)
      }
    }
  }
}

