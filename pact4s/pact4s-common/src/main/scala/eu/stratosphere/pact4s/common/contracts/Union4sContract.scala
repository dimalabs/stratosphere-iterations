package eu.stratosphere.pact4s.common.contracts

import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.stubs._

import eu.stratosphere.pact.common.contract._

trait Union4sContract[T] extends NoOp4sContract[T] { this: MapContract =>

  
}

object Union4sContract {

  def newBuilder = MapContract.builder(classOf[NoOp4sStub])

  def unapply(c: Union4sContract[_]) = Some(c.asInstanceOf[MapContract].getInputs())
}
