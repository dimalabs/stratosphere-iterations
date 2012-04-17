package eu.stratosphere.pact4s.example.graph

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._

/**
 * Transitive closure with recursive doubling.
 */
class TransitiveClosure(args: String*) extends PACTProgram {

  val vertices = new DataSource(params.verticesInput, params.delimeter, parseVertex)
  val edges = new DataSource(params.edgesInput, params.delimeter, parseEdge)
  val output = new DataSink(params.output, params.delimeter, formatOutput)

  val transitiveClosure = fixpointIncremental(step, more)(vertices, edges)

  override def outputs = output <~ transitiveClosure

  def more(c: DataStream[Int, Path], x: DataStream[Int, Path]) = x.nonEmpty

  def step(c: DataStream[Int, Path], x: DataStream[Int, Path]) = {

    val cNewPaths = x join c map joinPaths _
    val cByFromTo = c map { (from: Int, p: Path) => (p.from, p.to) --> p.dist }
    val c1 = cByFromTo cogroup cNewPaths map selectShortestDistance _

    val xByFrom = x map { (to: Int, p: Path) => p.from --> p }
    val xNewPaths = x join xByFrom map joinPaths _
    val x1 = xNewPaths cogroup c1 flatMap excludeKnownPaths _

    val c1Paths = c1 map { (edge: (Int, Int), dist: Int) => edge._1 --> Path(edge._1, edge._2, dist) }
    val x1Paths = x1 map { (edge: (Int, Int), dist: Int) => edge._2 --> Path(edge._1, edge._2, dist) }

    (c1Paths, x1Paths)
  }

  def joinPaths(v: Int, path1: Path, path2: Path) = (path1, path2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => (from, to) --> (dist1 + dist2)
  }

  def selectShortestDistance(edge: (Int, Int), dist1: Iterable[Int], dist2: Iterable[Int]) = edge --> (dist1 ++ dist2).min

  def excludeKnownPaths(edge: (Int, Int), x: Iterable[Int], c: Iterable[Int]) = c.toSeq match {
    case Seq() => x map { dist => edge --> dist }
    case _ => Seq()
  }

  override def name = "Transitive Closure with Recursive Doubling"
  override def description = "Parameters: [noSubStasks] [vertices] [edges] [output]"

  val params = new {
    val delimeter = "\n"
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val verticesInput = if (args.length > 1) args(1) else ""
    val edgesInput = if (args.length > 2) args(2) else ""
    val output = if (args.length > 3) args(3) else ""
  }

  override def getHints(item: Hintable) = item match {
    case vertices() => Degree(params.numSubTasks) +: UniqueKey +: ValuesPerKey(1) +: RecordSize(16)
    case edges() => Degree(params.numSubTasks) +: RecordSize(16)
    case output() => Degree(params.numSubTasks) +: UniqueKey +: ValuesPerKey(1) +: RecordSize(16)
  }

  case class Path(from: Int, to: Int, dist: Int)

  def parseVertex(line: String): Int --> Path = {
    val v = line.toInt
    v --> Path(v, v, 0)
  }

  val EdgeInputPattern = """(\d+)|(\d+)""".r

  def parseEdge(line: String): Int --> Path = line match {
    case EdgeInputPattern(from, to) => to.toInt --> Path(from.toInt, to.toInt, 1)
  }

  def formatOutput(vertex: Int, path: Path): String = path match {
    case Path(from, to, dist) => "%d|%d|%d".format(from, to, dist)
  }
}