package eu.stratosphere.pact4s.example.wordcount

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._

class WordCount(args: String*) extends PactProgram {

  val input = new DataSource(params.input, readLine)
  val output = new DataSink(params.output, formatOutput)

  val words = input flatMap { line => line.toLowerCase().split("""\W+""") map { (_, 1) } }
  val counts = words groupBy { case (word, _) => word } combine { values => (values.head._1, values map { _._2 } sum) }

  override def outputs = output <~ counts

  override def name = "Word Count"
  override def description = "Parameters: [noSubStasks] [input] [output]"
  override def defaultParallelism = params.numSubTasks

  val params = new {
    val numSubTasks = args(0).toInt
    val input = args(1)
    val output = args(2)
  }

  def readLine(line: String): String = line

  def formatOutput(wordWithCount: (String, Int)): String = wordWithCount match {
    case (word, count) => "%s %d".format(word, count)
  }
}