package eu.stratosphere.pact4s.examples.graph

import scala.math._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class ConnectedComponentsDescriptor extends PactDescriptor[ConnectedComponents] {
  override val name = "Connected Components"
  override val description = "Parameters: [numSubTasks] [vertices] [edges] [output]"
  override def getDefaultParallelism(args: Map[Int, String]) = args.getOrElse(0, "1").toInt

  override def createInstance(args: Map[Int, String]) = new ConnectedComponents(args.getOrElse(1, ""), args.getOrElse(2, ""), args.getOrElse(3, ""))
}

class ConnectedComponents(verticesInput: String, edgesInput: String, componentsOutput: String) extends PactProgram with ConnectedComponentsGeneratedImplicits {

  val vertices = new DataSource(verticesInput, DelimetedDataSourceFormat(parseVertex))
  val directedEdges = new DataSource(edgesInput, DelimetedDataSourceFormat(parseEdge))
  val output = new DataSink(componentsOutput, DelimetedDataSinkFormat(formatOutput.tupled))

  val undirectedEdges = directedEdges flatMap { case (from, to) => Seq(from -> to, to -> from) }

  //val components = vertices distinctBy { _._1 } untilEmpty vertices iterate propagateComponent
  val components = propagateComponent iterate (s0 = vertices distinctBy { _._1 }, ws0 = vertices)

  override def outputs = output <~ components

  def propagateComponent = (s: DataStream[(Int, Int)], ws: DataStream[(Int, Int)]) => {

    val allNeighbors = ws join undirectedEdges on { case (v, _) => v } isEqualTo { case (from, _) => from } map { case ((_, c), (_, to)) => to -> c }
    val minNeighbors = allNeighbors groupBy { case (to, _) => to } combine { cs => cs minBy { _._2 } }

    // updated solution elements == new workset
    val s1 = minNeighbors join s on { _._1 } isEqualTo { _._1 } flatMap {
      case ((v, cNew), (_, cOld)) if cNew < cOld => Some((v, cNew))
      case _                                     => None
    }

    (s1, s1)
  }

  vertices.hints = RecordSize(8)
  directedEdges.hints = RecordSize(8)
  undirectedEdges.hints = RecordSize(8) +: RecordsEmitted(2.0f)
  output.hints = RecordSize(8)

  def parseVertex = (line: String) => {
    val v = line.toInt
    v -> v
  }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => from.toInt -> to.toInt
  }

  def formatOutput = (vertex: Int, component: Int) => "%d|%d".format(vertex, component)
}

trait ConnectedComponentsGeneratedImplicits { this: ConnectedComponents =>

  import java.io.ObjectInputStream

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val intIntUDT: UDT[(Int, Int)] = new UDT[(Int, Int)] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactInteger])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Int)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactInteger()

      override def serialize(item: (Int, Int), record: PactRecord) = {
        val (v0, v1) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }
      }

      override def deserialize(record: PactRecord): (Int, Int) = {
        var v0: Int = 0
        var v1: Int = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1.getValue()
        }

        (v0, v1)
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactInteger()
      }
    }
  }

  implicit def udf1: UDF1[Function1[(Int, Int), Iterator[(Int, Int)]]] = defaultUDF1IterR[(Int, Int), (Int, Int)]
  implicit def udf2: UDF1[Function1[Iterator[(Int, Int)], (Int, Int)]] = defaultUDF1IterT[(Int, Int), (Int, Int)]
  implicit def udf3: UDF2[Function2[(Int, Int), (Int, Int), (Int, Int)]] = defaultUDF2[(Int, Int), (Int, Int), (Int, Int)]
  implicit def udf4: UDF2[Function2[(Int, Int), (Int, Int), Iterator[(Int, Int)]]] = defaultUDF2IterR[(Int, Int), (Int, Int), (Int, Int)]

  implicit def selOutput: FieldSelector[Function1[(Int, Int), Unit]] = defaultFieldSelectorT[(Int, Int), Unit]
  implicit def selFirst: FieldSelector[Function1[(Int, Int), Int]] = getFieldSelector[(Int, Int), Int](0)
}
