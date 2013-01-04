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

import eu.stratosphere.pact4s.common.analysis._

import eu.stratosphere.pact.common.contract._
import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.generic.io._

trait Pact4sContract[Out] { this: Contract =>

  def getUDF: UDF[Out]
  def getKeys: Seq[KeySelector[_]] = Seq()

  def persistConfiguration(classLoader: ClassLoader): Unit = {

    this.getParameters().setClassLoader(classLoader)

    def setKeys(getKeyColumnNumbers: Int => Array[Int]): Unit = {
      for ((key, inputNum) <- getKeys.zipWithIndex) {
        val source = key.selectedFields.toSerializerIndexArray
        val target = getKeyColumnNumbers(inputNum)
        assert(source.length == target.length, "Attempt to write " + source.length + " key indexes to an array of size " + target.length)
        System.arraycopy(source, 0, target, 0, source.length)
      }
    }

    this match {
      case contract: AbstractPact[_]  => setKeys(contract.getKeyColumnNumbers)
      case contract: WorksetIteration => setKeys(contract.getKeyColumnNumbers)
      case _ if getKeys.size > 0      => throw new UnsupportedOperationException("Attempted to set keys on a contract that doesn't support them")
      case _                          =>
    }

    this.persistConfiguration()
  }

  protected def persistConfiguration(): Unit = {}

  protected def annotations: Seq[Annotation] = Seq()

  override def getUserCodeAnnotation[A <: Annotation](annotationClass: Class[A]): A = {
    annotations find { _.annotationType().equals(annotationClass) } map { _.asInstanceOf[A] } getOrElse null.asInstanceOf[A]
  }
}

object Pact4sContract {
  implicit def toContract(c: Pact4sContract[_]): Contract = c.asInstanceOf[Contract]
  type OneInput = { def getInputs(): JList[Contract]; def setInput(c: Contract) }
  type TwoInput = { def getFirstInputs(): JList[Contract]; def setFirstInput(c: Contract); def getSecondInputs(): JList[Contract]; def setSecondInput(c: Contract) }
}

trait Pact4sOneInputContract[In, Out] extends Pact4sContract[Out] { this: Contract with Pact4sContract.OneInput =>

  val udf: UDF1[In, Out]
  override def getUDF = udf

  def singleInput = this.getInputs().get(0)
  def singleInput_=(value: Pact4sContract[In]) = this.setInput(value)
}

trait Pact4sOneInputKeyedContract[Key, In, Out] extends Pact4sOneInputContract[In, Out] { this: Contract with Pact4sContract.OneInput =>

  val key: KeySelector[In => Key]
  override def getKeys = Seq(key)
}

trait Pact4sTwoInputContract[LeftIn, RightIn, Out] extends Pact4sContract[Out] { this: Contract with Pact4sContract.TwoInput =>

  val udf: UDF2[LeftIn, RightIn, Out]
  override def getUDF = udf

  def leftInput = this.getFirstInputs().get(0)
  def leftInput_=(left: Pact4sContract[LeftIn]) = this.setFirstInput(left)

  def rightInput = this.getSecondInputs().get(0)
  def rightInput_=(right: Pact4sContract[RightIn]) = this.setSecondInput(right)
}

trait Pact4sTwoInputKeyedContract[Key, LeftIn, RightIn, Out] extends Pact4sTwoInputContract[LeftIn, RightIn, Out] { this: Contract with Pact4sContract.TwoInput =>

  val leftKey: KeySelector[LeftIn => Key]
  val rightKey: KeySelector[RightIn => Key]
  override def getKeys = Seq(leftKey, rightKey)
}

trait NoOp4sContract[T] extends Pact4sContract[T] { this: Contract =>

  val udf: UDF0[T]
  override def getUDF = udf
}

trait NoOp4sKeyedContract[Key, T] extends NoOp4sContract[T] { this: Contract =>
  val key: KeySelector[T => Key]
  override def getKeys = Seq(key)
}

trait DataSource4sContract[Out] extends Pact4sContract[Out] { this: GenericDataSource[_ <: InputFormat[_, _]] =>

  val udf: UDF0[Out]
  override def getUDF = udf
}

trait DataSink4sContract[In] extends Pact4sOneInputContract[In, Nothing] { this: GenericDataSink =>

}

object DataSink4sContract {
  implicit def toGenericSink(s: DataSink4sContract[_]): GenericDataSink = s.asInstanceOf[GenericDataSink]
  implicit def toGenericSinks(s: Seq[DataSink4sContract[_]]): JCollection[GenericDataSink] = s.map(_.asInstanceOf[GenericDataSink])

  def unapply(c: DataSink4sContract[_]) = Some(c.singleInput)
}

