package eu.stratosphere.pact4s.common.analyzer

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.postpass.OptimizerPostPass;
import eu.stratosphere.pact.compiler.plan._;

trait GlobalSchemaOptimizer extends OptimizerPostPass {

  override def postPass(plan: OptimizedPlan): Unit = {
    plan.getDataSinks().foreach(node => discardUnusedFields(node, Set[Int]()))
  }

  private def discardUnusedFields(node: OptimizerNode, parentReads: Set[Int]): Unit = {

    node match {

      case node: DataSinkNode => node.getPactContract match {
        case DataSink4sContract(_, _, fieldSelector) => {

          val reads = fieldSelector.getFields.toFieldSet ++ parentReads
          discardUnusedFields(node.getPredNode(), reads)
        }
      }

      case node: DataSourceNode => node.getPactContract match {
        case DataSource4sContract(_, fieldSelector) => {

          fieldSelector.discardUnusedFields(parentReads)
        }
      }

      case node: CoGroupNode => node.getPactContract match {
        case CoGroup4sContract(_, _, leftKey, rightKey, _, _, _, udf) => {

          udf.discardUnusedOutputFields(parentReads)

          val leftReads = leftKey.getFields.toFieldSet ++ udf.getReadFields._1.toFieldSet ++ parentReads
          val rightReads = rightKey.getFields.toFieldSet ++ udf.getReadFields._2.toFieldSet ++ parentReads

          discardUnusedFields(node.getFirstPredNode(), leftReads)
          discardUnusedFields(node.getSecondPredNode(), rightReads)
        }
      }

      case node: CrossNode => node.getPactContract match {
        case Cross4sContract(_, _, _, _, _, udf) => {

          udf.discardUnusedOutputFields(parentReads)

          val leftReads = udf.getReadFields._1.toFieldSet ++ parentReads
          val rightReads = udf.getReadFields._2.toFieldSet ++ parentReads

          discardUnusedFields(node.getFirstPredNode(), leftReads)
          discardUnusedFields(node.getSecondPredNode(), rightReads)
        }
      }

      case node: MapNode => node.getPactContract match {

        case Map4sContract(_, _, _, udf) => {

          udf.discardUnusedOutputFields(parentReads)

          val reads = udf.getReadFields.toFieldSet ++ parentReads
          discardUnusedFields(node.getPredNode(), reads)
        }

        case Copy4sContract(_, udf) => {

          udf.discardUnusedOutputFields(parentReads)

          val reads = udf.getReadFields.toFieldSet ++ parentReads
          discardUnusedFields(node.getPredNode(), reads)
        }

        case Union4sContract(_, _, udf) => {

          discardUnusedFields(node.getPredNode(), parentReads)
        }
      }

      case node: MatchNode => node.getPactContract match {
        case Join4sContract(_, _, leftKey, rightKey, _, _, _, udf) => {

          udf.discardUnusedOutputFields(parentReads)

          val leftReads = leftKey.getFields.toFieldSet ++ udf.getReadFields._1.toFieldSet ++ parentReads
          val rightReads = rightKey.getFields.toFieldSet ++ udf.getReadFields._2.toFieldSet ++ parentReads

          discardUnusedFields(node.getFirstPredNode(), leftReads)
          discardUnusedFields(node.getSecondPredNode(), rightReads)
        }
      }

      case node: ReduceNode => node.getPactContract match {
        case Reduce4sContract(_, key, _, _, cUDF, rUDF) => {

          rUDF.discardUnusedOutputFields(parentReads)

          val reads = cUDF.getReadFields.toFieldSet ++ rUDF.getReadFields.toFieldSet ++ parentReads
          discardUnusedFields(node.getPredNode(), reads)
        }
      }

      case _: SinkJoiner | _: UnionNode | _: CombinerNode => {
        node.getIncomingConnections().foreach(conn => discardUnusedFields(conn.getSourcePact(), parentReads))
      }
    }
  }

  private implicit def array2FieldSetOps(fields: Array[Int]): { def toFieldSet: Set[Int] } = new {
    def toFieldSet: Set[Int] = fields.filter(_ > -1).toSet
  }

  private implicit def seq2FieldSetOps(fields: Seq[(Int, Int)]): { def toFieldSet: Set[Int] } = new {
    def toFieldSet: Set[Int] = fields.map(_._2).filter(_ > -1).toSet
  }
}
