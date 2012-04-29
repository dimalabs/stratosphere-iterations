package eu.stratosphere.pact4s.common

import java.lang.reflect.Constructor

import eu.stratosphere.pact4s.common.analyzer.GlobalSchemaGenerator

import eu.stratosphere.pact.common.plan._

abstract class PactDescriptor[T <: PactProgram: Manifest] extends PlanAssembler with PlanAssemblerDescription with GlobalSchemaGenerator {

  val name: String = implicitly[Manifest[T]].toString
  val description: String

  def getName(args: String*) = name
  override def getDescription = description

  override def getPlan(args: String*): Plan = {

    val program = createInstance(args: _*)
    val sinks = program.outputs map { _.getContract }

    initGlobalSchema(sinks)

    new Plan(sinks, getName(args: _*)) {
      this.setDefaultParallelism(program.defaultParallelism)
    }
  }

  def createInstance(args: String*): T = {

    val clazz = implicitly[Manifest[T]].erasure

    try {

      val constr = optionally { clazz.getConstructor(classOf[Seq[String]]).asInstanceOf[Constructor[Any]] }
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