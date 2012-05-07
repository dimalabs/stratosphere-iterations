package eu.stratosphere.pact4s.common.contracts

import java.lang.annotation.Annotation
import java.util.{ Collection => JCollection }
import java.util.{ List => JList }

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.io._

trait Pact4sContract { this: Contract =>

  var outDegree = 0

  def persistConfiguration() = {}

  protected def annotations: Seq[Annotation] = Seq()

  override def getUserCodeAnnotation[A <: Annotation](annotationClass: Class[A]): A = {
    annotations find { _.annotationType().equals(annotationClass) } map { _.asInstanceOf[A] } getOrElse null.asInstanceOf[A]
  }
}

object Pact4sContract {
  implicit def toContract(c: Pact4sContract): Contract = c
}

trait Pact4sOneInputContract extends Pact4sContract { this: Contract with Pact4sOneInputContract.OneInput =>
  def singleInput = this.getInputs().get(0)
  def singleInput_=(input: Pact4sContract) = this.setInput(input)
}

object Pact4sOneInputContract {

  type OneInput = { def getInputs(): JList[Contract]; def setInput(c: Contract) }

  def unapply(c: Pact4sOneInputContract) = Some(c.singleInput)
}

trait Pact4sTwoInputContract extends Pact4sContract { this: Contract with Pact4sTwoInputContract.TwoInput =>
  def leftInput = this.getFirstInputs().get(0)
  def leftInput_=(left: Pact4sContract) = this.setFirstInput(left)

  def rightInput = this.getSecondInputs().get(0)
  def rightInput_=(right: Pact4sContract) = this.setSecondInput(right)
}

object Pact4sTwoInputContract {

  type TwoInput = { def getFirstInputs(): JList[Contract]; def setFirstInput(c: Contract); def getSecondInputs(): JList[Contract]; def setSecondInput(c: Contract) }

  def unapply(c: Pact4sTwoInputContract) = Some((c.leftInput, c.rightInput))
}

trait Pact4sDataSourceContract[Out] extends Pact4sContract { this: GenericDataSource[_ <: InputFormat[_]] =>

  val outputUDT: UDT[Out]
  val fieldSelector: FieldSelector[_ => Out]

  override def annotations = Seq(new Annotations.ExplicitModifications(fieldSelector.getFields))
}

object Pact4sDataSourceContract {

  def unapply(c: Pact4sDataSourceContract[_]) = Some((c.outputUDT, c.fieldSelector))
}

trait Pact4sDataSinkContract[In] extends Pact4sOneInputContract { this: GenericDataSink =>

  val inputUDT: UDT[In]
  val fieldSelector: FieldSelector[In => _]

  override def annotations = Seq(new Annotations.Reads(fieldSelector.getFields))
}

object Pact4sDataSinkContract {
  implicit def toGenericSink(s: Pact4sDataSinkContract[_]): GenericDataSink = s
  implicit def toGenericSinks(s: Seq[Pact4sDataSinkContract[_]]): JCollection[GenericDataSink] = s

  def unapply(c: Pact4sDataSinkContract[_]) = Some((c.singleInput, c.inputUDT, c.fieldSelector))
}

