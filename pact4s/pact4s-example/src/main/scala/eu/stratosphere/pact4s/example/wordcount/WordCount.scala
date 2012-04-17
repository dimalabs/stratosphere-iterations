package eu.stratosphere.pact4s.example.wordcount

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._

class WordCount(args: String*) extends PACTProgram {

  val input = new DataSource(params.input, params.delimeter, readLine)
  val output = new DataSink(params.output, params.delimeter, formatOutput)

  val words = input flatMap { (_: Unit, line: String) => line.toLowerCase().split("""\W+""").toSeq map { _ --> 1 } }
  val counts = words combine { (word: String, values: Iterable[Int]) => values.sum } map { (word: String, count: Int) => count }

  override def outputs = output <~ counts

  override def name = "Word Count"
  override def description = "Parameters: [noSubStasks] [input] [output]"

  val params = new {
    val delimeter = "\n"
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val input = if (args.length > 1) args(1) else ""
    val output = if (args.length > 2) args(2) else ""
  }

  override def getHints(item: Hintable) = item match {
    case input() => Degree(params.numSubTasks)
    case words() => Degree(params.numSubTasks)
    case counts() => Degree(params.numSubTasks)
    case output() => Degree(params.numSubTasks)
  }

  def readLine(line: String): Unit --> String = () --> line
  def formatOutput(word: String, count: Int): String = "%s %d".format(word, count)
}