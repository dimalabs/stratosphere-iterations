package eu.stratosphere.pact4s.example.wordcount

import scala.collection.JavaConversions._
import scala.math._

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.util.Iterator

import eu.stratosphere.pact.common.contract.FileDataSinkContract
import eu.stratosphere.pact.common.contract.FileDataSourceContract
import eu.stratosphere.pact.common.contract.OutputContract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.io.TextInputFormat
import eu.stratosphere.pact.common.io.TextOutputFormat
import eu.stratosphere.pact.common.plan.Plan
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.pact.common.stub.Collector
import eu.stratosphere.pact.common.stub.MapStub
import eu.stratosphere.pact.common.stub.ReduceStub
import eu.stratosphere.pact.common.`type`.KeyValuePair
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactNull
import eu.stratosphere.pact.common.`type`.base.PactString

class WordCountScalaPlain extends PlanAssembler with PlanAssemblerDescription {

  import WordCountScalaPlain._

  override def getPlan(args: String*): Plan = {
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val dataInput = if (args.length > 1) args(1) else ""
    val output = if (args.length > 2) args(2) else ""

    val data = new FileDataSourceContract(classOf[LineInFormat], dataInput, "Input Lines")
    data.setDegreeOfParallelism(numSubTasks)

	val mapper = new MapContract(classOf[TokenizeLine], "Tokenize Lines");
	mapper.setDegreeOfParallelism(numSubTasks)

	val reducer = new ReduceContract(classOf[CountWords], "Count Words");
	reducer.setDegreeOfParallelism(numSubTasks)

	val out = new FileDataSinkContract(classOf[WordCountOutFormat], output, "Word Counts");
	out.setDegreeOfParallelism(numSubTasks)

	out.setInput(reducer);
	reducer.setInput(mapper);
	mapper.setInput(data);

    new Plan(out, "WordCount Example")
  }

  override def getDescription() = "Parameters: [noSubStasks] [input] [output]"
}

object WordCountScalaPlain {
    
  class TokenizeLine extends MapStub[PactNull, PactString, PactString, PactInteger] {
    
    override def map(key: PactNull, value: PactString, out: Collector[PactString, PactInteger]) = {
        
      for (val word <- value.toString().toLowerCase().split("""\W+""")) {
        out.collect(new PactString(word), new PactInteger(1))
      }
    }
  }

  @OutputContract.SameKey
  @ReduceContract.Combinable
  class CountWords extends ReduceStub[PactString, PactInteger, PactString, PactInteger] {

    override def reduce(key: PactString, values: Iterator[PactInteger], out: Collector[PactString, PactInteger]) = {
      val sum = values.map(_.getValue()).fold(0)(_ + _)
      out.collect(key, new PactInteger(sum))
    }

    override def combine(key: PactString, values: Iterator[PactInteger], out: Collector[PactString, PactInteger]) = {
      reduce(key, values, out)
    }
  }

  class LineInFormat extends TextInputFormat[PactNull, PactString] {

    override def readLine(pair: KeyValuePair[PactNull, PactString], line: Array[Byte]): Boolean = {
        pair.setKey(new PactNull())
        pair.setValue(new PactString(new String(line)))
        true
    }
  }

  class WordCountOutFormat extends TextOutputFormat[PactString, PactInteger] {

    override def writeLine(pair: KeyValuePair[PactString, PactInteger]): Array[Byte] = {
      val key = pair.getKey().getValue()
      val value = pair.getValue().getValue()

      "%s %d\n".format(key, value).getBytes()
    }
  }
}
