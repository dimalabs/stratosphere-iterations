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

import scala.collection.JavaConversions._

import java.util.Iterator

import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.FileDataSource
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.plan.Plan
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.stubs.ReduceStub
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactString

class WordCountPlain extends PlanAssembler with PlanAssemblerDescription {

  import WordCountPlain._

  override def getPlan(args: String*): Plan = {
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val dataInput = if (args.length > 1) args(1) else ""
    val output = if (args.length > 2) args(2) else ""

    val data = new FileDataSource(classOf[LineInFormat], dataInput, "Input Lines")
    val mapper = new MapContract(classOf[TokenizeLine], data, "Tokenize Lines");
    val reducer = new ReduceContract(classOf[CountWords], classOf[PactString], 0, mapper, "Count Words");
    val out = new FileDataSink(classOf[WordCountOutFormat], output, reducer, "Word Counts");

    val plan = new Plan(out, "WordCount Example")
    plan.setDefaultParallelism(numSubTasks)

    plan
  }

  override def getDescription() = "Parameters: [noSubStasks] [input] [output]"
}

object WordCountPlain {

  class TokenizeLine extends MapStub {

    // String -> [(String, Int)]
    override def map(lineRecord: PactRecord, out: Collector[PactRecord]) = {
      val res = new PactRecord(2)
      val line = lineRecord.getField(0, classOf[PactString]).getValue()

      for (word <- line.toString().toLowerCase().split("""\W+""")) {
        res.setField(0, word)
        res.setField(1, 1)
        out.collect(res)
      }
    }
  }

  @ReduceContract.Combinable
  class CountWords extends ReduceStub {

    // [(String, Int)] -> (String, Int)
    override def reduce(counts: Iterator[PactRecord], out: Collector[PactRecord]) = {
      val res = counts.next()
      val sum = counts map { _.getField(1, classOf[PactInteger]).getValue() } sum

      res.setField(1, sum)
      out.collect(res)
    }

    // [(String, Int)] -> (String, Int)
    override def combine(counts: Iterator[PactRecord], out: Collector[PactRecord]) = {
      reduce(counts, out)
    }
  }

  class LineInFormat extends DelimitedInputFormat {

    override def readRecord(record: PactRecord, line: Array[Byte], numBytes: Int): Boolean = {
      record.setNumFields(1)
      record.setField(0, new PactString(new String(line)))
      true
    }
  }

  class WordCountOutFormat extends DelimitedOutputFormat {

    override def serializeRecord(record: PactRecord, target: Array[Byte]): Int = {
      val word = record.getField(0, classOf[PactString]).getValue()
      val count = record.getField(1, classOf[PactInteger]).getValue()

      val byteString = "%s %d".format(word, count).getBytes()

      if (byteString.length <= target.length) {
        System.arraycopy(byteString, 0, target, 0, byteString.length);
        byteString.length;
      } else {
        -byteString.length;
      }
    }
  }

  implicit def int2PactInteger(x: Int): PactInteger = new PactInteger(x)
  implicit def pactInteger2Int(x: PactInteger): Int = x.getValue()
  implicit def string2PactString(x: String): PactString = new PactString(x)
  implicit def pactString2String(x: PactString): String = x.getValue()
}
