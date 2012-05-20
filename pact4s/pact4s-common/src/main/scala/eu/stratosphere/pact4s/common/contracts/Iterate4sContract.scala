package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact.common.contract._

trait Iterate4sContract[SolutionItem] extends Pact4sContract { this: Iteration =>

}

object Iterate4sContract {

  def unapply(c: Iterate4sContract[_]) = {
    val iter = c.asInstanceOf[Iteration]
    Some((iter.getInitialPartialSolution(), iter.getNextPartialSolution(), iter.getTerminationCriterion(), iter.getPartialSolution()))
  }
}

trait WorksetIterate4sContract[Key, SolutionItem, WorksetItem] extends Pact4sContract { this: WorksetIteration =>

  val keySelector: FieldSelector[SolutionItem => Key]
}

object WorksetIterate4sContract {

  def unapply(c: WorksetIterate4sContract[_, _, _]) = {
    val iter = c.asInstanceOf[WorksetIteration]
    Some((iter.getInitialPartialSolution(), iter.getInitialWorkset(), c.keySelector, iter.getPartialSolutionDelta(), iter.getNextWorkset(), iter.getPartialSolution(), iter.getWorkset()))
  }
}