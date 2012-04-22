package eu.stratosphere.pact4s.example.graph

import scala.math._
import eu.stratosphere.pact4s.common._

class ConnectedComponents(args: String*) extends PactProgram {

  val vertices = new DataSource(params.verticesInput, parseVertex)
  val directedEdges = new DataSource(params.edgesInput, parseEdge)
  val output = new DataSink(params.output, formatOutput)

  val undirectedEdges = directedEdges flatMap { case (from, to) => Seq(from -> to, to -> from) }
  val components = vertices untilEmpty vertices iterate propagateComponent

  override def outputs = output <~ components

  def propagateComponent(s: DataStream[(Int, Int)], ws: DataStream[(Int, Int)]) = {

    val allNeighbors = ws join undirectedEdges on { case (v, _) => v } isEqualTo { case (from, _) => from } map { case ((_, c), (_, to)) => to -> c }
    val minNeighbors = allNeighbors groupBy { case (to, _) => to } combine { cs => (cs.head._1, cs map { case (_, c) => c } min) }

    // updated solution elements == new workset
    val s1 = minNeighbors join s on { _._1 } isEqualTo { _._1 } flatMap {
      case ((v, cNew), (_, cOld)) if cNew < cOld => Some((v, cNew))
      case _ => None
    }

    (s1, s1)
  }

  override def name = "Connected Components"
  override def description = "Parameters: [noSubStasks] [vertices] [edges] [output]"
  override def defaultParallelism = params.numSubTasks
  
  vertices.hints = UniqueKey +: RecordSize(8)
  directedEdges.hints = RecordSize(8)
  undirectedEdges.hints = RecordSize(8) +: Selectivity(2.0f)
  output.hints = UniqueKey +: RecordSize(8)

  val params = new {
    val numSubTasks = args(0).toInt
    val verticesInput = args(1)
    val edgesInput = args(2)
    val output = args(3)
  }

  def parseVertex(line: String): (Int, Int) = {
    val v = line.toInt
    v -> v
  }

  val EdgeInputPattern = """(\d+)|(\d+)""".r

  def parseEdge(line: String): (Int, Int) = line match {
    case EdgeInputPattern(from, to) => from.toInt -> to.toInt
  }

  def formatOutput(value: (Int, Int)): String = "%d|%d".format(value._1, value._2)
}