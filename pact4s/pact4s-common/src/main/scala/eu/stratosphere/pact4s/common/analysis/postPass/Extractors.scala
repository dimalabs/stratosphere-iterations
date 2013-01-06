package eu.stratosphere.pact4s.common.analysis.postPass

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.plan._

object Extractors {

  implicit def nodeToGetUDF(node: OptimizerNode) = new {
    def getUDF: Option[UDF[_]] = node match {
      case _: SinkJoiner | _: UnionNode | _: CombinerNode => None
      case _ => {
        Some(node.getPactContract.asInstanceOf[Pact4sContract[_]].getUDF)
      }
    }
  }

  object DataSinkNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: DataSinkNode => node.getPactContract match {
        case contract: DataSink4sContract[_] => Some((contract.getUDF, node.getInConn))
        case _                               => None
      }
      case _ => None
    }
  }

  object DataSourceNode {
    def unapply(node: OptimizerNode): Option[(UDF0[_])] = node match {
      case node: DataSourceNode => node.getPactContract match {
        case contract: DataSource4sContract[_] => Some(contract.getUDF)
        case _                                 => None
      }
      case _ => None
    }
  }

  object CoGroupNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], KeySelector[_ <: Function1[_, _]], KeySelector[_ <: Function1[_, _]], PactConnection, PactConnection)] = node match {
      case node: CoGroupNode => node.getPactContract match {
        case contract: CoGroup4sContract[_, _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstInConn, node.getSecondInConn))
        case _                                       => None
      }
      case _ => None
    }
  }

  object CrossNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], PactConnection, PactConnection)] = node match {
      case node: CrossNode => node.getPactContract match {
        case contract: Cross4sContract[_, _, _] => Some((contract.getUDF, node.getFirstInConn, node.getSecondInConn))
        case _                                  => None
      }
      case _ => None
    }
  }

  object JoinNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], KeySelector[_ <: Function1[_, _]], KeySelector[_ <: Function1[_, _]], PactConnection, PactConnection)] = node match {
      case node: MatchNode => node.getPactContract match {
        case contract: Join4sContract[_, _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstInConn, node.getSecondInConn))
        case _                                    => None
      }
      case _ => None
    }
  }

  object MapNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: MapNode => node.getPactContract match {
        case contract: Map4sContract[_, _] => Some((contract.getUDF, node.getInConn))
        case contract: Copy4sContract[_]   => Some((contract.getUDF, node.getInConn))
        case _                             => None
      }
      case _ => None
    }
  }

  object ReduceNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], KeySelector[_ <: Function1[_, _]], PactConnection)] = node match {
      case node: ReduceNode => node.getPactContract match {
        case contract: Reduce4sContract[_, _, _] => Some((contract.getUDF, contract.key, node.getInConn))
        case _                                   => None
      }
      case _ => None
    }
  }
}

