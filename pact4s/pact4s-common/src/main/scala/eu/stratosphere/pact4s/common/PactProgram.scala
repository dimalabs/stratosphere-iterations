package eu.stratosphere.pact4s.common

import java.lang.reflect.Constructor
import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.plan._

abstract class PactProgram {

  type DataStream[T] = eu.stratosphere.pact4s.common.streams.DataStream[T]
  type DataSink[T, S] = eu.stratosphere.pact4s.common.streams.DataSink[T, S]
  type DataSource[S, T, F] = eu.stratosphere.pact4s.common.streams.DataSource[S, T, F]

  def defaultParallelism = 1
  def outputs: Seq[PlanOutput]
}

abstract class PactDescriptor[T <: PactProgram: Manifest] extends PlanAssembler with PlanAssemblerDescription {

  val name: String = implicitly[Manifest[T]].toString
  val description: String

  def getName(args: String*) = name
  override def getDescription = description

  override def getPlan(args: String*): Plan = {
    val program = createInstance(args: _*)
    val outputs = program.outputs

    null
  }

  def createInstance(args: String*): T = {

    try {
      val constr = implicitly[Manifest[T]].erasure.getConstructor(classOf[Seq[String]])
      constr.newInstance(Array[Object](args)).asInstanceOf[T]
    } catch {
      case e: NoSuchMethodException => {
        implicitly[Manifest[T]].erasure.newInstance.asInstanceOf[T]
      }
    }
  }
}