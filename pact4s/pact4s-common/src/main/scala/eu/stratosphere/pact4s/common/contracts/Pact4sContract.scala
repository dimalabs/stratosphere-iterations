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

import java.lang.annotation.Annotation
import java.util.{ Collection => JCollection }
import java.util.{ List => JList }

import scala.collection.JavaConversions._

import eu.stratosphere.pact4s.common.analyzer._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.generic.io._

trait Pact4sContract { this: Contract =>

  var outDegree = 0

  def persistConfiguration() = {}

  protected def annotations: Seq[Annotation] = Seq()

  override def getUserCodeAnnotation[A <: Annotation](annotationClass: Class[A]): A = {
    annotations find { _.annotationType().equals(annotationClass) } map { _.asInstanceOf[A] } getOrElse null.asInstanceOf[A]
  }
}

object Pact4sContract {
  implicit def toContract(c: Pact4sContract): Contract = c.asInstanceOf[Contract]
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

trait DataSource4sContract[Out] extends Pact4sContract { this: GenericDataSource[_ <: InputFormat[_, _]] =>

  val outputUDT: UDT[Out]
  val fieldSelector: FieldSelector[_ => Out]
}

object DataSource4sContract {

  def unapply(c: DataSource4sContract[_]) = Some((c.outputUDT, c.fieldSelector))
}

trait DataSink4sContract[In] extends Pact4sOneInputContract { this: GenericDataSink =>

  val inputUDT: UDT[In]
  val fieldSelector: FieldSelector[In => _]
}

object DataSink4sContract {
  implicit def toGenericSink(s: DataSink4sContract[_]): GenericDataSink = s.asInstanceOf[GenericDataSink]
  implicit def toGenericSinks(s: Seq[DataSink4sContract[_]]): JCollection[GenericDataSink] = s.map(_.asInstanceOf[GenericDataSink])

  def unapply(c: DataSink4sContract[_]) = Some((c.singleInput, c.inputUDT, c.fieldSelector))
}

