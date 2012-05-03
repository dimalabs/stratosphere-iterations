package eu.stratosphere.pact4s.examples.graph

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

/**
 * Transitive Closure with Recursive Doubling
 */
object TransitiveClosureRD extends PactDescriptor[TransitiveClosureRD] {
  override val name = "Transitive Closure with Recursive Doubling"
  override val description = "Parameters: [noSubStasks] [vertices] [edges] [output]"
}

class TransitiveClosureRD(args: String*) extends PactProgram with TransitiveClosureRDGeneratedImplicits {

  val vertices = new DataSource(params.verticesInput, DelimetedDataSourceFormat(parseVertex _))
  val edges = new DataSource(params.edgesInput, DelimetedDataSourceFormat(parseEdge _))
  val output = new DataSink(params.output, DelimetedDataSinkFormat(formatOutput _))

  val transitiveClosure = vertices keyBy getEdge untilEmpty edges iterate createClosure

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

  def selectShortestDistance(dist1: Iterator[Path], dist2: Iterator[Path]) = (dist1 ++ dist2) minBy { _.dist }

  def excludeKnownPaths(x: Iterator[Path], c: Iterator[Path]) = {
    if (c.isEmpty)
      x
    else
      Iterable.empty
  } 

  def getEdge(p: Path): (Int, Int) = (p.from, p.to)
  def getFrom(p: Path): Int = p.from
  def getTo(p: Path): Int = p.to

  override def defaultParallelism = params.numSubTasks

  vertices.hints = RecordSize(16)
  edges.hints = RecordSize(16)
  output.hints = RecordSize(16)

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

trait TransitiveClosureRDGeneratedImplicits { this: TransitiveClosureRD =>

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val pathSerializer: UDT[Path] = new UDT[Path] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactInteger], classOf[PactInteger])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Path] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)

      private val w0 = new PactInteger()
      private val w1 = new PactInteger()
      private val w2 = new PactInteger()

      override def serialize(item: Path, record: PactRecord) = {
        val Path(v0, v1, v2) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }

        if (ix2 >= 0) {
          w2.setValue(v2)
          record.setField(ix2, w2)
        }
      }

      override def deserialize(record: PactRecord): Path = {

        var v0: Int = 0
        var v1: Int = 0
        var v2: Int = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1.getValue()
        }

        if (ix2 >= 0) {
          record.getFieldInto(ix2, w2)
          v2 = w2.getValue()
        }

        Path(v0, v1, v2)
      }
    }
  }
}