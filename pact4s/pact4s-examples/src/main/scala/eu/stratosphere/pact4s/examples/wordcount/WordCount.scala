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

package eu.stratosphere.pact4s.examples.wordcount

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class WordCountDescriptor extends PactDescriptor[WordCount] {
  override val name = "Word Count"
  override val description = "Parameters: [numSubTasks] [input] [output]"
  override def getDefaultParallelism(args: Map[Int, String]) = args.getOrElse(0, "1").toInt

  override def createInstance(args: Map[Int, String]) = new WordCount(args.getOrElse(1, "input"), args.getOrElse(2, "output"))
}

class WordCount(textInput: String, wordsOutput: String) extends PactProgram with WordCountGeneratedImplicits {

  val input = new DataSource(textInput, DelimetedDataSourceFormat(identity[String] _))
  val output = new DataSink(wordsOutput, DelimetedDataSinkFormat(formatOutput.tupled))

  val words = input flatMap { _.toLowerCase().split("""\W+""") map { (_, 1) } }

  val counts = words groupBy { case (word, _) => word } combine {
    _.reduce { (z, s) => z.copy(_2 = z._2 + s._2) }
  }

  override def outputs = output <~ counts

  input.hints = PactName("Input")
  output.hints = PactName("Output")
  words.hints = PactName("Words")
  counts.hints = PactName("Counts")

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)
}

trait WordCountGeneratedImplicits { this: WordCount =>

  import java.io.ObjectInputStream

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  /*
  implicit val stringSerializer: UDT[String] = new UDT[String] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactString])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[String] with Serializable {

      private val ix0 = indexMap(0)

      @transient private var w0 = new PactString()

      override def serialize(item: String, record: PactRecord) = {

        if (ix0 >= 0) {
          w0.setValue(item)
          record.setField(ix0, w0)
        }
      }

      override def deserialize(record: PactRecord): String = {
        var v0: String = null

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        v0
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactString()
      }
    }
  }

  implicit val stringIntSerializer: UDT[(String, Int)] = new UDT[(String, Int)] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactString], classOf[PactInteger])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(String, Int)] with Serializable {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)

      @transient private var w0 = new PactString()
      @transient private var w1 = new PactInteger()

      override def serialize(item: (String, Int), record: PactRecord) = {
        val (v0, v1) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }
      }

      override def deserialize(record: PactRecord): (String, Int) = {
        var v0: String = null
        var v1: Int = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1.getValue()
        }

        (v0, v1)
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactString()
        w1 = new PactInteger()
      }
    }
  }
  */

  implicit def udf1: UDF1[Function1[String, Iterator[String]]] = defaultUDF1IterR[String, String]
  implicit def udf2: UDF1[Function1[String, Iterator[(String, Int)]]] = defaultUDF1IterR[String, (String, Int)]
  implicit def udf3: UDF1[Function1[Iterator[(String, Int)], (String, Int)]] = defaultUDF1IterT[(String, Int), (String, Int)]

  implicit def selOutput: FieldSelector[Function1[(String, Int), Unit]] = defaultFieldSelectorT[(String, Int), Unit]
  implicit def selCounts: FieldSelector[Function1[(String, Int), String]] = getFieldSelector[(String, Int), String](0)
}