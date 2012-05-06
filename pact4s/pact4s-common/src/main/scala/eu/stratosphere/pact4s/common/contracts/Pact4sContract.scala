package eu.stratosphere.pact4s.common.contracts

import java.lang.annotation.Annotation
import java.util.Collection

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.io._

trait Pact4sContract { this: Contract =>

  def persistConfiguration() = {}

  def annotations: Seq[Annotation] = Seq()

  override def getUserCodeAnnotation[A <: Annotation](annotationClass: Class[A]): A = {
    annotations find { _.annotationType().equals(annotationClass) } map { _.asInstanceOf[A] } getOrElse null.asInstanceOf[A]
  }
}

object Pact4sContract {
  implicit def toContract(c: Pact4sContract): Contract = c
}

trait Pact4sDataSourceContract[Out] extends Pact4sContract { this: GenericDataSource[_ <: InputFormat[_]] =>

  val outputUDT: UDT[Out]
  val fieldSelector: FieldSelector[_ => Out]

  override val annotations = Seq(new Annotations.ExplicitModifications(fieldSelector.getFields))
}

object Pact4sDataSourceContract {

  def unapply(c: Pact4sDataSourceContract[_]) = Some((c.outputUDT, c.fieldSelector))
}

trait Pact4sDataSinkContract[In] extends Pact4sContract { this: GenericDataSink =>

  def input = this.getInputs().get(0)
  def input_=(input: Pact4sContract) = this.setInput(input)

  val inputUDT: UDT[In]
  val fieldSelector: FieldSelector[In => _]

  override val annotations = Seq(new Annotations.Reads(fieldSelector.getFields))
}

object Pact4sDataSinkContract {
  implicit def toGenericSink(s: Pact4sDataSinkContract[_]): GenericDataSink = s
  implicit def toGenericSinks(s: Seq[Pact4sDataSinkContract[_]]): Collection[GenericDataSink] = s

  def unapply(c: Pact4sDataSinkContract[_]) = Some((c.input, c.inputUDT, c.fieldSelector))
}

