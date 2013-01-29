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

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.contracts._

import eu.stratosphere.pact.compiler.plan._

object Extractors {

  implicit def nodeToGetUDF(node: OptimizerNode) = new {
    def getUDF: Option[UDF[_]] = node match {
      case _: SinkJoiner | _: BinaryUnionNode => None
      case _ => {
        Some(node.getPactContract.asInstanceOf[Pact4sContract[_]].getUDF)
      }
    }
  }

  object DataSinkNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: DataSinkNode => node.getPactContract match {
        case contract: DataSink4sContract[_] => Some((contract.getUDF, node.getInputConnection()))
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
        case contract: CoGroup4sContract[_, _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection(), node.getSecondIncomingConnection()))
        case _                                       => None
      }
      case _ => None
    }
  }

  object CrossNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], PactConnection, PactConnection)] = node match {
      case node: CrossNode => node.getPactContract match {
        case contract: Cross4sContract[_, _, _] => Some((contract.getUDF, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _                                  => None
      }
      case _ => None
    }
  }

  object JoinNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], KeySelector[_ <: Function1[_, _]], KeySelector[_ <: Function1[_, _]], PactConnection, PactConnection)] = node match {
      case node: MatchNode => node.getPactContract match {
        case contract: Join4sContract[_, _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _                                    => None
      }
      case _ => None
    }
  }

  object MapNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: MapNode => node.getPactContract match {
        case contract: Map4sContract[_, _] => Some((contract.getUDF, node.getIncomingConnection))
        case contract: Copy4sContract[_]   => Some((contract.getUDF, node.getIncomingConnection))
        case _                             => None
      }
      case _ => None
    }
  }

  object ReduceNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], KeySelector[_ <: Function1[_, _]], PactConnection)] = node match {
      case node: ReduceNode => node.getPactContract match {
        case contract: Reduce4sContract[_, _, _] => Some((contract.getUDF, contract.key, node.getIncomingConnection))
        case _                                   => None
      }
      case _ => None
    }
  }
}

