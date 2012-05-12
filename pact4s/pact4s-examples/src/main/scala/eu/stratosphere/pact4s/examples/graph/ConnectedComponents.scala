package eu.stratosphere.pact4s.examples.graph

import scala.math._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class ConnectedComponentsDescriptor extends PactDescriptor[ConnectedComponents] {
  override val name = "Connected Components"
  override val description = "Parameters: [noSubStasks] [vertices] [edges] [output]"
}

class ConnectedComponents(args: String*) extends PactProgram with ConnectedComponentsGeneratedImplicits {

  val vertices = new DataSource(params.verticesInput, DelimetedDataSourceFormat(parseVertex _))
  val directedEdges = new DataSource(params.edgesInput, DelimetedDataSourceFormat(parseEdge _))
  val output = new DataSink(params.output, DelimetedDataSinkFormat(formatOutput _))

  val undirectedEdges = directedEdges flatMap { case (from, to) => Seq(from -> to, to -> from) }
  val components = vertices keyBy { _._1 } untilEmpty vertices iterate propagateComponent

  override def outputs = output <~ components

  def propagateComponent(s: DataStream[(Int, Int)], ws: DataStream[(Int, Int)]) = {

    val allNeighbors = ws join undirectedEdges on { case (v, _) => v } isEqualTo { case (from, _) => from } map { case ((_, c), (_, to)) => to -> c }
    val minNeighbors = allNeighbors groupBy { case (to, _) => to } combine { cs => cs minBy { _._2 } }

    // updated solution elements == new workset
    val s1 = minNeighbors join s on { _._1 } isEqualTo { _._1 } flatMap {
      case ((v, cNew), (_, cOld)) if cNew < cOld => Some((v, cNew))
      case _                                     => None
    }

    (s1, s1)
  }

  override def defaultParallelism = params.numSubTasks

  vertices.hints = RecordSize(8)
  directedEdges.hints = RecordSize(8)
  undirectedEdges.hints = RecordSize(8) +: RecordsEmitted(2.0f)
  output.hints = RecordSize(8)

  def params = {
    val argMap = args.zipWithIndex.map (_.swap).toMap

    new {
      val numSubTasks = argMap.getOrElse(0, "0").toInt
      val verticesInput = argMap.getOrElse(1, "")
      val edgesInput = argMap.getOrElse(2, "")
      val output = argMap.getOrElse(3, "")
    }
  }

  def parseVertex(line: String): (Int, Int) = {
    val v = line.toInt
    v -> v
  }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  def parseEdge(line: String): (Int, Int) = line match {
    case EdgeInputPattern(from, to) => from.toInt -> to.toInt
  }

  def formatOutput(value: (Int, Int)): String = "%d|%d".format(value._1, value._2)
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

  implicit val udf1: UDF1[Function1[(Int, Int), Iterator[(Int, Int)]]] = defaultUDF1IterR[(Int, Int), (Int, Int)]
  implicit val udf2: UDF1[Function1[Iterator[(Int, Int)], (Int, Int)]] = defaultUDF1IterT[(Int, Int), (Int, Int)]
  implicit val udf3: UDF2[Function2[(Int, Int), (Int, Int), (Int, Int)]] = defaultUDF2[(Int, Int), (Int, Int), (Int, Int)]
  implicit val udf4: UDF2[Function2[(Int, Int), (Int, Int), Iterator[(Int, Int)]]] = defaultUDF2IterR[(Int, Int), (Int, Int), (Int, Int)]

  implicit val selOutput: FieldSelector[Function1[(Int, Int), Unit]] = defaultFieldSelectorT[(Int, Int), Unit]
  implicit val selFirst: FieldSelector[Function1[(Int, Int), Int]] = getFieldSelector[(Int, Int), Int](0)
}
