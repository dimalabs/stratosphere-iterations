/**
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

class WordCount(textInput: String, wordsOutput: String) extends PactProgram {

  val input = new DataSource(textInput, DelimetedDataSourceFormat(identity[String] _))
  val output = new DataSink(wordsOutput, DelimetedDataSinkFormat(formatOutput.tupled))

  val words = input flatMap { _.toLowerCase().split("""\W+""") map { (_, 1) } }

  val counts = words groupBy { case (word, _) => word } combine {
    _.reduce { (z, s) => z.copy(_2 = z._2 + s._2) }
  }
  
  counts.ignores { case (word, _) => word }
  counts.preserves { case (word, _) => word } as { case (word, _) => word }

  override def outputs = output <~ counts

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)
}

