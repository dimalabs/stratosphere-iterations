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

package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact.common.contract._

trait Iterate4sContract[SolutionItem] extends NoOp4sContract[SolutionItem] { this: Iteration =>

}

object Iterate4sContract {

  def unapply(c: Iterate4sContract[_]) = {
    val iter = c.asInstanceOf[Iteration]
    Some((iter.getInitialPartialSolution(), iter.getNextPartialSolution(), Option(iter.getTerminationCriterion()), iter.getPartialSolution()))
  }
}

trait WorksetIterate4sContract[SolutionKey, SolutionItem, WorksetItem] extends NoOp4sKeyedContract[SolutionKey, SolutionItem] { this: WorksetIteration =>

}

object WorksetIterate4sContract {

  def unapply(c: WorksetIterate4sContract[_, _, _]) = {
    val iter = c.asInstanceOf[WorksetIteration]
    Some((iter.getInitialPartialSolution(), iter.getInitialWorkset(), iter.getPartialSolutionDelta(), iter.getNextWorkset(), iter.getPartialSolution(), iter.getWorkset()))
  }
}