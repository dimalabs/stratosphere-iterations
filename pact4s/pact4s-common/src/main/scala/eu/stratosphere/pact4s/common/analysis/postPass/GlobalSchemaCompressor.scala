package eu.stratosphere.pact4s.common.analysis.postPass

import scala.collection.mutable
import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.plan._

object GlobalSchemaCompressor {

  import Extractors._

  def compressSchema(plan: OptimizedPlan): Unit = {

    val (_, conflicts) = plan.getDataSinks().foldLeft((Set[OptimizerNode](), Map[GlobalPos, Set[GlobalPos]]())) {
      case ((visited, conflicts), node) => findConflicts(node, visited, conflicts)
    }

    // Reset all position indexes before reassigning them 
    conflicts.keys.foreach { _.setIndex(Int.MinValue) }

    plan.getDataSinks().foldLeft(Set[OptimizerNode]())(compressSchema(conflicts))
  }

  private def findConflicts(node: OptimizerNode, visited: Set[OptimizerNode], conflicts: Map[GlobalPos, Set[GlobalPos]]): (Set[OptimizerNode], Map[GlobalPos, Set[GlobalPos]]) = {

    // breadth-first traversal
    node.getOutConns.map(_.getTargetPact).forall(visited) match {

      case false => (visited, conflicts)

      case true => {

        val newVisited = visited + node
        val newConflicts = findConflicts(node, conflicts)

        node.getIncomingConnections.map(_.getSourcePact).foldLeft((newVisited, newConflicts)) {
          case ((visited, conflicts), node) => findConflicts(node, visited, conflicts)
        }
      }
    }
  }

  /**
   * Two fields are in conflict when they exist in the same place (record) at the same time (plan node).
   * If two fields are in conflict, then they must be assigned different indexes.
   *
   * p1 conflictsWith p2 =
   *   Exists(n in Nodes):
   *     p1 != p2 &&
   *     (
   *       (p1 in n.forwards && p2 in n.forwards) ||
   *       (p1 in n.forwards && p2 in n.outputs) ||
   *       (p2 in n.forwards && p1 in n.outputs)
   *     )
   */
  private def findConflicts(node: OptimizerNode, conflicts: Map[GlobalPos, Set[GlobalPos]]): Map[GlobalPos, Set[GlobalPos]] = {

    val (forwardPos, outputFields) = node.getUDF match {
      case None                     => (Set[GlobalPos](), Set[OutputField]())
      case Some(udf: UDF0[_])       => (Set[GlobalPos](), udf.outputFields.toSet)
      case Some(udf: UDF1[_, _])    => (udf.forwardSet, udf.outputFields.toSet)
      case Some(udf: UDF2[_, _, _]) => (udf.leftForwardSet ++ udf.rightForwardSet, udf.outputFields.toSet)
    }

    // resolve GlobalPos references to the instance that holds the actual index 
    val forwards = forwardPos map { _.resolve }
    val outputs = outputFields filter { _.isUsed } map { _.globalPos.resolve }

    val newConflicts = forwards.foldLeft(conflicts) {
      case (conflicts, fPos) => {
        // add all other forwards and all outputs to this forward's conflict set
        val fConflicts = conflicts.getOrElse(fPos, Set()) ++ (forwards filterNot { _ == fPos }) ++ outputs
        conflicts.updated(fPos, fConflicts)
      }
    }

    outputs.foldLeft(newConflicts) {
      case (conflicts, oPos) => {
        // add all forwards to this output's conflict set
        val oConflicts = conflicts.getOrElse(oPos, Set()) ++ forwards
        conflicts.updated(oPos, oConflicts)
      }
    }
  }

  /**
   * Assign indexes bottom-up, giving lower values to fields with larger conflict sets.
   * This ordering should do a decent job of minimizing the number of gaps between fields.
   */
  private def compressSchema(conflicts: Map[GlobalPos, Set[GlobalPos]])(visited: Set[OptimizerNode], node: OptimizerNode): Set[OptimizerNode] = {

    visited.contains(node) match {

      case true => visited

      case false => {

        node.getIncomingConnections.map(_.getSourcePact).foldLeft(visited)(compressSchema(conflicts))

        val outputFields = node.getUDF match {
          case None      => Seq[OutputField]()
          case Some(udf) => udf.outputFields filter { _.isUsed }
        }

        val outputs = outputFields map {
          case field => {
            val pos = field.globalPos.resolve
            (pos, field.localPos, conflicts(pos) map { _.getValue })
          }
        } sortBy {
          case (_, localPos, posConflicts) => (Int.MaxValue - posConflicts.size, localPos)
        }

        val initUsed = outputs map { _._1.getValue } filter { _ >= 0 } toSet

        val used = outputs.filter(_._1.getValue < 0).foldLeft(initUsed) {
          case (used, (pos, _, conflicts)) => {
            val index = chooseIndexValue(used ++ conflicts)
            pos.setIndex(index)
            used + index
          }
        }

        node.getUDF match {
          case Some(udf: UDF1[_, _])    => updateDiscards(used, udf.discardSet)
          case Some(udf: UDF2[_, _, _]) => updateDiscards(used, udf.leftDiscardSet, udf.rightDiscardSet)
          case _                        =>
        }

        visited + node
      }
    }
  }

  private def chooseIndexValue(used: Set[Int]): Int = {
    var index = 0
    while (used(index)) {
      index = index + 1
    }
    index
  }

  private def updateDiscards(outputs: Set[Int], discardSets: mutable.Set[GlobalPos]*): Unit = {
    for (discardSet <- discardSets) {
      for (discard <- discardSet if outputs.contains(discard.getValue)) {
        discardSet.remove(discard)
      }
    }
  }
}

