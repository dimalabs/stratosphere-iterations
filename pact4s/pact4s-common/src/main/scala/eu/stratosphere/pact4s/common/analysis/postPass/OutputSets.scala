package eu.stratosphere.pact4s.common.analysis.postPass

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.plan._

object OutputSets {

  import Extractors._

  def computeOutputSets(plan: OptimizedPlan): (Map[OptimizerNode, Set[Int]], Map[Int, GlobalPos]) = {
    
    val root = plan.getDataSinks.map(s => s: OptimizerNode).reduceLeft((n1, n2) => new SinkJoiner(n1, n2))
    val outputSets = computeOutputSets(Map[OptimizerNode, Set[GlobalPos]](), root)
    val outputPositions = outputSets(root).map(pos => (pos.getValue, pos)).toMap
    
    (outputSets.mapValues(_.map(_.getValue)), outputPositions)
  }

  private def computeOutputSets(outputSets: Map[OptimizerNode, Set[GlobalPos]], node: OptimizerNode): Map[OptimizerNode, Set[GlobalPos]] = {

    outputSets.contains(node) match {

      case true => outputSets

      case false => {

        val children = node.getIncomingConnections.map(_.getSourcePact).toSet
        val newOutputSets = children.foldLeft(outputSets)(computeOutputSets)
        
        val childOutputs = children.map(newOutputSets(_)).flatten
        val nodeOutputs = node.getUDF map { _.outputFields.filter(_.isUsed).map(_.globalPos).toSet } getOrElse Set()
        
        newOutputSets.updated(node, childOutputs ++ nodeOutputs)
      }
    }
  }
}

