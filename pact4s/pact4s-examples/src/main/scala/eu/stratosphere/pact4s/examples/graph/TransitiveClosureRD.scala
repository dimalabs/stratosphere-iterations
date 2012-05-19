package eu.stratosphere.pact4s.examples.graph

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

/**
 * Transitive Closure with Recursive Doubling
 */
class TransitiveClosureRDDescriptor extends PactDescriptor[TransitiveClosureRD] {
  override val name = "Transitive Closure with Recursive Doubling"
  override val description = "Parameters: [numSubTasks] [vertices] [edges] [output]"
  override def getDefaultParallelism(args: Map[Int, String]) = args.getOrElse(0, "1").toInt

  override def createInstance(args: Map[Int, String]) = new TransitiveClosureRD(args.getOrElse(1, ""), args.getOrElse(2, ""), args.getOrElse(3, ""))
}

class TransitiveClosureRD(verticesInput: String, edgesInput: String, pathsOutput: String) extends PactProgram with TransitiveClosureRDGeneratedImplicits {

  val vertices = new DataSource(verticesInput, DelimetedDataSourceFormat(parseVertex))
  val edges = new DataSource(edgesInput, DelimetedDataSourceFormat(parseEdge))
  val output = new DataSink(pathsOutput, DelimetedDataSinkFormat(formatOutput))

  //val transitiveClosure = vertices distinctBy getEdge untilEmpty edges iterate createClosure
  val transitiveClosure = createClosure iterate (s0 = vertices distinctBy getEdge, ws0 = edges)

  override def outputs = output <~ transitiveClosure

  val createClosure = (c: DataStream[Path], x: DataStream[Path]) => {

    val cNewPaths = x.join(c).on(getTo)(selTo).isEqualTo(getFrom)(selFrom) map joinPaths
    val c1 = cNewPaths cogroup cNewPaths on getEdge isEqualTo getEdge map selectShortestDistance

    val xNewPaths = x.join(x).on(getTo)(selTo).isEqualTo(getFrom)(selFrom) map joinPaths
    val x1 = xNewPaths cogroup c1 on getEdge isEqualTo getEdge flatMap excludeKnownPaths

    (c1, x1)
  }

  val selectShortestDistance = (dist1: Iterator[Path], dist2: Iterator[Path]) => (dist1 ++ dist2) minBy { _.dist }

  val excludeKnownPaths = (x: Iterator[Path], c: Iterator[Path]) => {
    if (c.isEmpty)
      x
    else
      Iterator.empty
  }

  val joinPaths = (p1: Path, p2: Path) => (p1, p2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2)
  }

  val getEdge = (p: Path) => (p.from, p.to)
  val getFrom = (p: Path) => p.from
  val getTo = (p: Path) => p.to

  vertices.hints = RecordSize(16)
  edges.hints = RecordSize(16)
  output.hints = RecordSize(16)

  case class Path(from: Int, to: Int, dist: Int)

  val parseVertex = (line: String) => {
    val v = line.toInt
    Path(v, v, 0)
  }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  val parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }

  val formatOutput = (path: Path) => "%d|%d|%d".format(path.from, path.to, path.dist)
}

trait TransitiveClosureRDGeneratedImplicits { this: TransitiveClosureRD =>

  import java.io.ObjectInputStream

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val pathSerializer: UDT[Path] = new UDT[Path] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactInteger], classOf[PactInteger])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Path] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactInteger()
      @transient private var w2 = new PactInteger()

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

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactInteger()
        w2 = new PactInteger()
      }
    }
  }

  implicit val udf1: UDF2[Function2[Path, Path, Path]] = defaultUDF2[Path, Path, Path]
  implicit val udf2: UDF2[Function2[Iterator[Path], Iterator[Path], Path]] = defaultUDF2IterT[Path, Path, Path]
  implicit val udf3: UDF2[Function2[Iterator[Path], Iterator[Path], Iterator[Path]]] = defaultUDF2IterTR[Path, Path, Path]

  implicit val selOutput: FieldSelector[Function1[Path, Unit]] = defaultFieldSelectorT[Path, Unit]
  implicit val selEdge: FieldSelector[Function1[Path, (Int, Int)]] = getFieldSelector[Path, (Int, Int)](0, 1)
  val selFrom: FieldSelector[Function1[Path, Int]] = getFieldSelector[Path, Int](0)
  val selTo: FieldSelector[Function1[Path, Int]] = getFieldSelector[Path, Int](1)
}
