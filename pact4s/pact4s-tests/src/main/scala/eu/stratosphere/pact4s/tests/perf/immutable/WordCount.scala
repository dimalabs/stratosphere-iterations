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

package eu.stratosphere.pact4s.tests.perf.immutable

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class WordCountDescriptor extends PactDescriptor[WordCount] {
  override val name = "Word Count (Immutable)"
  override val parameters = "-input <file> -output <file>"

  override def createInstance(args: Pact4sArgs) = new WordCount(args("input"), args("output"))
}

class WordCount(textInput: String, wordsOutput: String) extends PactProgram {

  val input = new DataSource(textInput, DelimetedDataSourceFormat(identity[String] _))
  val output = new DataSink(wordsOutput, DelimetedDataSinkFormat(formatOutput.tupled))

  val words = input flatMap { _.toLowerCase().split("""\W+""") map { (_, 1) } }
  val counts = words groupBy { case (word, _) => word } combine { _ reduce addCounts }

  override def outputs = output <~ counts

  def addCounts(w1: (String, Int), w2: (String, Int)) = (w1._1, w1._2 + w2._2)
  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  counts neglects { case (word, _) => word }
  counts preserves { case (word, _) => word } as { case (word, _) => word }
}
