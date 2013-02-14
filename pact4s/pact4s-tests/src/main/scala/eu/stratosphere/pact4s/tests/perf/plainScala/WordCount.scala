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

package eu.stratosphere.pact4s.tests.perf.plainScala

import java.util.Iterator

import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.FileDataSource
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.stubs.ReduceStub
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.pact.common.util.MutableObjectIterator;

class WordCount extends PlanAssembler with PlanAssemblerDescription {

  import WordCount._

  override def getPlan(args: String*): Plan = {

    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val dataInput = if (args.length > 1) args(1) else ""
    val output = if (args.length > 2) args(2) else ""

    val source = new FileDataSource(classOf[TextInputFormat], dataInput, "Input Lines")
    source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII")

    val mapper = MapContract.builder(classOf[TokenizeLine])
      .input(source).name("Tokenize Lines").build()

    val reducer = ReduceContract.builder(classOf[CountWords], classOf[PactString], 0)
      .input(mapper).name("Count Words").build()

    val out = new FileDataSink(classOf[RecordOutputFormat], output, reducer, "Word Counts");

    RecordOutputFormat.configureRecordFormat(out).lenient(true)
      .recordDelimiter('\n').fieldDelimiter(' ')
      .field(classOf[PactString], 0)
      .field(classOf[PactInteger], 1)

    val plan = new Plan(out, "WordCount")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }

  override def getDescription() = "Parameters: [numSubStasks] [input] [output]"
}

object WordCount {

  object AsciiUtils {
    def toLowerCase(string: PactString) = {
      val chars = string.getCharArray
      val len = string.length
      var i = 0

      while (i < len) {
        chars(i) = Character.toLowerCase(chars(i))
        i = i + 1
      }
    }

    def replaceNonWordChars(string: PactString, replacement: Char) = {
      val chars = string.getCharArray
      val len = string.length
      var i = 0

      while (i < len) {
        val c = chars(i)
        if (!(Character.isLetter(c) || Character.isDigit(c) || c == '_')) {
          chars(i) = replacement
        }
        i = i + 1
      }
    }

    final class WhitespaceTokenizer extends MutableObjectIterator[PactString] {
      private var toTokenize: PactString = _
      private var pos: Int = _
      private var limit: Int = _

      def setStringToTokenize(string: PactString) = {
        this.toTokenize = string
        this.pos = 0
        this.limit = string.length
      }

      override def next(target: PactString): Boolean = {
        val data = this.toTokenize.getCharArray
        val limit = this.limit
        var pos = this.pos

        while (pos < limit && Character.isWhitespace(data(pos))) {
          pos = pos + 1
        }

        val hasNext = pos < limit

        if (hasNext) {

          val start = pos
          while (pos < limit && !Character.isWhitespace(data(pos))) {
            pos = pos + 1
          }

          target.setValue(this.toTokenize, start, pos - start)
        }

        this.pos = pos
        hasNext
      }
    }
  }

  class TokenizeLine extends MapStub {

    private val line = new PactString()
    private val word = new PactString()
    private val one = new PactInteger(1)
    private val result = new PactRecord()

    private val tokenizer = new AsciiUtils.WhitespaceTokenizer()

    override def map(record: PactRecord, out: Collector[PactRecord]) = {

      val line = record.getField(0, this.line)

      AsciiUtils.replaceNonWordChars(line, ' ')
      AsciiUtils.toLowerCase(line)
      this.tokenizer.setStringToTokenize(line)

      while (tokenizer.next(this.word)) {
        this.result.setField(0, this.word)
        this.result.setField(1, this.one)
        out.collect(this.result)
      }
    }
  }

  @ConstantFields(fields = Array(0))
  @OutCardBounds(lowerBound = 1, upperBound = 1)
  @Combinable
  class CountWords extends ReduceStub {

    private val count = new PactInteger()

    override def reduce(records: Iterator[PactRecord], out: Collector[PactRecord]) = {

      var next: PactRecord = null
      var count = 0

      while (records.hasNext()) {
        next = records.next()
        count += next.getField(1, this.count).getValue()
      }

      this.count.setValue(count)
      next.setField(1, this.count)

      out.collect(next)
    }

    override def combine(records: Iterator[PactRecord], out: Collector[PactRecord]) = {
      reduce(records, out)
    }
  }
}
