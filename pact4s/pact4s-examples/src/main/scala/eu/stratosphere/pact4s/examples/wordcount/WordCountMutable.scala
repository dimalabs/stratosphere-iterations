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

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class WordCountMutableDescriptor extends PactDescriptor[WordCountMutable] {
  override val name = "Word Count (Mutable)"
  override val parameters = "-input <file> -output <file>"

  override def createInstance(args: Pact4sArgs) = new WordCountMutable(args("input"), args("output"))
}

class WordCountMutable(textInput: String, wordsOutput: String) extends PactProgram {

  val input = new DataSource(textInput, DelimetedDataSourceFormat(identity[String] _))
  val output = new DataSink(wordsOutput, DelimetedDataSinkFormat(formatOutput _))

  val words = input flatMap { _.toLowerCase().split("""\W+""") map { WordWithCount(_, 1) } }
  val counts = words groupBy { case WordWithCount(word, _) => word } combine { _ reduce addCounts }

  counts neglects { case WordWithCount(word, _) => word }
  counts preserves { case WordWithCount(word, _) => word } as { case WordWithCount(word, _) => word }

  override def outputs = output <~ counts

  case class WordWithCount(var word: String, var count: Int)

  def addCounts(w1: WordWithCount, w2: WordWithCount) = WordWithCount(w1.word, w1.count + w2.count)
  def formatOutput(w: WordWithCount) = "%s %d".format(w.word, w.count)
}

