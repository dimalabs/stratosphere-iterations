package eu.stratosphere.pact4s.example.graph

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._

class TransitiveClosureRD(args: String*) extends PactProgram {

  val vertices = new DataSource(params.verticesInput, parseVertex)
  val edges = new DataSource(params.edgesInput, parseEdge)
  val output = new DataSink(params.output, formatOutput)

  val transitiveClosure = vertices untilEmpty edges iterate createClosure

  override def outputs = output <~ transitiveClosure

  def createClosure(c: DataStream[Path], x: DataStream[Path]) = {

    val cNewPaths = x join c on getTo isEqualTo getFrom map joinPaths
    val c1 = cNewPaths cogroup cNewPaths on getEdge isEqualTo getEdge map selectShortestDistance

    val xNewPaths = x join x on getTo isEqualTo getFrom map joinPaths
    val x1 = xNewPaths cogroup c1 on getEdge isEqualTo getEdge flatMap excludeKnownPaths

    (c1, x1)
  }

  def joinPaths(p1: Path, p2: Path) = (p1, p2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2)
  }

  def selectShortestDistance(dist1: Iterable[Path], dist2: Iterable[Path]) = (dist1 ++ dist2) minBy { _.dist }

  def excludeKnownPaths(x: Iterable[Path], c: Iterable[Path]) = c.toSeq match {
    case Seq() => x
    case _ => Seq()
  }

  def getEdge(p: Path): (Int, Int) = (p.from, p.to)
  def getFrom(p: Path): Int = p.from
  def getTo(p: Path): Int = p.to

  override def name = "Transitive Closure with Recursive Doubling"
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