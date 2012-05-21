package eu.stratosphere.pact4s.common

import java.lang.reflect.Constructor

import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.plan._

abstract class PactDescriptor[T <: PactProgram: Manifest] extends PlanAssembler with PlanAssemblerDescription with GlobalSchemaGenerator {

  val name: String = implicitly[Manifest[T]].toString
  val description: String

  def getName(args: Map[Int, String]) = name
  def getDefaultParallelism(args: Map[Int, String]) = 1
  override def getDescription = description

  override def getPlan(args: String*): Plan = {

    implicit val env = new Environment
    val argsMap = args.zipWithIndex.map (_.swap).toMap

    val program = createInstance(argsMap)
    val sinks = program.outputs map { _.getContract.asInstanceOf[DataSink4sContract[_]] }

    initGlobalSchema(sinks)

    val plan = new Plan(sinks, getName(argsMap))
    plan.setDefaultParallelism(getDefaultParallelism(argsMap))
    plan
  }

  def createInstance(args: Map[Int, String]): T = {

    val clazz = implicitly[Manifest[T]].erasure

    try {

      val constr = optionally { clazz.getConstructor(classOf[Map[Int, String]]).asInstanceOf[Constructor[Any]] }
      val inst = constr map { _.newInstance(args) } getOrElse { clazz.newInstance }

      inst.asInstanceOf[T]

    } catch {
      case e => throw new Pact4sInstantiationException(e)
    }
  }

  private def optionally[S](block: => S): Option[S] = {
    try {
      Some(block)
    } catch {
      case _ => None
    }
  }
}

class Pact4sInstantiationException(cause: Throwable) extends Exception("Could not instantiate program.", cause)