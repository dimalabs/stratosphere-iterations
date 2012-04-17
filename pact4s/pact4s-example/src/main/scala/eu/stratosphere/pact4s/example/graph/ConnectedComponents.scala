package eu.stratosphere.pact4s.example.graph

import scala.math._
import eu.stratosphere.pact4s.common._

class ConnectedComponents(args: String*) extends PACTProgram {

  val vertices = new DataSource(params.verticesInput, params.delimeter, parseVertex)
  val directedEdges = new DataSource(params.edgesInput, params.delimeter, parseEdge)
  val output = new DataSink(params.output, params.delimeter, formatOutput)

  val undirectedEdges = directedEdges flatMap { (from: Int, to: Int) => Seq(from --> to, to --> from) }
  val components = fixpointIncremental(step, more)(vertices, vertices)

  override def outputs = output <~ components

  def more(s: DataStream[Int, Int], ws: DataStream[Int, Int]) = ws.nonEmpty

  def step(s: DataStream[Int, Int], ws: DataStream[Int, Int]) = {

    val n = ws join undirectedEdges map { (from: Int, c: Int, to: Int) => to --> c }

    val nc = s cogroup n map { (v: Int, oldC: Iterable[Int], newCs: Iterable[Int]) =>
      oldC.toSeq match {
        case Seq(oldC) => {
          val newC = min(oldC, newCs.min)
          v --> (newC, newC != oldC)
        }
      }
    }

    val s1 = nc map { (v: Int, c: (Int, Boolean)) => c._1 }
    val ws1 = nc filter { (v: Int, c: (Int, Boolean)) => c._2 } map { (v: Int, c: (Int, Boolean)) => c._1 }

    (s1, ws1)
  }

  override def name = "Connected Components"
  override def description = "Parameters: [noSubStasks] [vertices] [edges] [output]"

  val params = new {
    val delimeter = "\n"
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val verticesInput = if (args.length > 1) args(1) else ""
    val edgesInput = if (args.length > 2) args(2) else ""
    val output = if (args.length > 3) args(3) else ""
  }

  override def getHints(item: Hintable) = item match {
    case vertices() => Degree(params.numSubTasks) +: UniqueKey +: ValuesPerKey(1) +: RecordSize(8)
    case directedEdges() => Degree(params.numSubTasks) +: RecordSize(8)
    case undirectedEdges() => Degree(params.numSubTasks) +: RecordSize(8) +: Selectivity(2.0f)
    case output() => Degree(params.numSubTasks) +: UniqueKey +: ValuesPerKey(1) +: RecordSize(8)
  }

  def parseVertex(line: String): Int --> Int = {
    val v = line.toInt
    v --> v
  }

  val EdgeInputPattern = """(\d+)|(\d+)""".r

  def parseEdge(line: String): Int --> Int = line match {
    case EdgeInputPattern(from, to) => from.toInt --> to.toInt
  }

  def formatOutput(vertex: Int, component: Int): String = "%d|%d".format(vertex, component)
}