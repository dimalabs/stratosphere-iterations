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

    val plan = new Plan(out, "WordCount Example")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }

  override def getDescription() = "Parameters: [noSubStasks] [input] [output]"
}

object WordCount {

  class TokenizeLine extends MapStub {

    val outputRecord = new PactRecord()
    val line = new PactString()
    val word = new PactString()
    val one = new PactInteger(1)

    override def map(record: PactRecord, out: Collector[PactRecord]) = {

      record.getField(0, line)

      for (w <- line.getValue().toLowerCase().split("""\W+""")) {
        word.setValue(w)
        outputRecord.setField(0, word)
        outputRecord.setField(1, one)
        out.collect(outputRecord)
      }
    }
  }

  @Combinable
  @ConstantFields(fields = Array(0))
  @OutCardBounds(lowerBound = 1, upperBound = 1)
  class CountWords extends ReduceStub {

    val cnt = new PactInteger()

    override def reduce(records: Iterator[PactRecord], out: Collector[PactRecord]) = {

      var element: PactRecord = null
      var sum = 0

      while (records.hasNext()) {
        element = records.next()
        element.getField(1, cnt)
        sum += cnt.getValue()
      }

      cnt.setValue(sum)
      element.setField(1, cnt)
      out.collect(element)
    }

    override def combine(records: Iterator[PactRecord], out: Collector[PactRecord]) = {
      reduce(records, out)
    }
  }
}
