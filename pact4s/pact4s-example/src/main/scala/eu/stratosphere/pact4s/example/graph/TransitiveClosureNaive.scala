package eu.stratosphere.pact4s.example.graph

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._

class TransitiveClosureNaive(args: String*) extends PactProgram {

  val vertices = new DataSource(params.verticesInput, parseVertex)
  val edges = new DataSource(params.edgesInput, parseEdge)
  val output = new DataSink(params.output, formatOutput)

  val transitiveClosure = vertices iterate createClosure

  override def outputs = output <~ transitiveClosure

  def createClosure(paths: DataStream[Path]) = {

    val allNewPaths = paths join edges on getTo isEqualTo getFrom map joinPaths
    val shortestPaths = allNewPaths groupBy getEdge combine { _ minBy { _.dist } }

    shortestPaths
  }

  def joinPaths(p1: Path, p2: Path) = (p1, p2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2)
  }

  def getEdge(p: Path): (Int, Int) = (p.from, p.to)
  def getFrom(p: Path): Int = p.from
  def getTo(p: Path): Int = p.to

  override def name = "Transitive Closure (Naive)"
  override def description = "Parameters: [noSubStasks] [vertices] [edges] [output]"
  override def defaultParallelism = params.numSubTasks

  vertices.hints = UniqueKey +: RecordSize(16)
  edges.hints = RecordSize(16)
  output.hints = UniqueKey +: RecordSize(16)

  val params = new {
    val numSubTasks = args(0).toInt
    val verticesInput = args(1)
    val edgesInput = args(2)
    val output = args(3)
  }

  case class Path(from: Int, to: Int, dist: Int)

  def parseVertex(line: String): Path = {
    val v = line.toInt
    Path(v, v, 0)
  }

  val EdgeInputPattern = """(\d+)|(\d+)""".r

  def parseEdge(line: String): Path = line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }

  def formatOutput(path: Path): String = path match {
    case Path(from, to, dist) => "%d|%d|%d".format(from, to, dist)
  }
}