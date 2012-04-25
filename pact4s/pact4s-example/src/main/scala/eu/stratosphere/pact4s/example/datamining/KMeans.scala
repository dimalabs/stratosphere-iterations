package eu.stratosphere.pact4s.example.datamining

import scala.math._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class KMeans(args: String*) extends PactProgram with KMeansGeneratedImplicits {

  val dataPoints = new DataSource(params.dataPointInput, parseInput)
  val clusterPoints = new DataSource(params.clusterInput, parseInput)
  val newClusterPoints = new DataSink(params.output, formatOutput)

  val distances = dataPoints cross clusterPoints map computeDistance
  val nearestCenters = distances groupBy { case (pid, _) => pid } combine { ds => ds.minBy(_._2.distance) } map asPointSum
  val newCenters = nearestCenters groupBy { case (cid, _) => cid } combine sumPointSums map { case (cid: Int, pSum: PointSum) => cid -> pSum.toPoint() }

  override def outputs = newClusterPoints <~ newCenters

  def computeDistance(p: (Int, Point), c: (Int, Point)): (Int, Distance) = (p, c) match {
    case ((pid, dataPoint), (cid, clusterPoint)) => {
      val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)
      pid -> Distance(dataPoint, cid, distToCluster)
    }
  }

  def asPointSum(value: (Int, Distance)): (Int, PointSum) = value match {
    case (_, Distance(dataPoint, clusterId, _)) => clusterId -> PointSum(1, dataPoint)
  }

  def sumPointSums(dataPoints: Iterable[(Int, PointSum)]): (Int, PointSum) = {
    val points = dataPoints map { _._2 }
    dataPoints.head._1 -> points.fold(PointSum(0, Point(0, 0, 0)))(_ + _)
  }

  override def name = "KMeans Iteration"
  override def description = "Parameters: [noSubStasks] [dataPoints] [clusterCenters] [output]"
  override def defaultParallelism = params.numSubTasks

  clusterPoints.hints = Degree(1)
  distances.hints = RecordSize(48)
  nearestCenters.hints = RecordSize(48)
  newCenters.hints = RecordSize(36)

  val params = new {
    val numSubTasks = args(0).toInt
    val dataPointInput = args(1)
    val clusterInput = args(2)
    val output = args(3)
  }

  val PointInputPattern = """(\d+)|(\d+\.\d+)|(\d+\.\d+)|(\d+\.\d+)|""".r

  def parseInput(line: String): (Int, Point) = line match {
    case PointInputPattern(id, x, y, z) => id.toInt -> Point(x.toDouble, y.toDouble, z.toDouble)
  }

  def formatOutput(value: (Int, Point)): String = value match {
    case (cid, Point(x, y, z)) => "%d|%.2f|%.2f|%.2f|".format(cid, x, y, z)
  }
}

case class Point(x: Double, y: Double, z: Double) {
  def computeEuclidianDistance(other: Point) = other match {
    case Point(x2, y2, z2) => sqrt(pow(x - x2, 2) + pow(y - y2, 2) + pow(z - z2, 2))
  }
}

case class Distance(dataPoint: Point, clusterId: Int, distance: Double)

case class PointSum(count: Int, pointSum: Point) {
  def +(that: PointSum) = that match {
    case PointSum(c, Point(x, y, z)) => PointSum(count + c, Point(x + pointSum.x, y + pointSum.y, z + pointSum.z))
  }

  def toPoint() = Point(pointSum.x / count, pointSum.y / count, pointSum.z / count)
}

trait KMeansGeneratedImplicits { this: KMeans =>

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val intPointSerializer: UDT[(Int, Point)] = new UDT[(Int, Point)] {

    override val fieldCount = 4

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Point)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)
      private val ix3 = indexMap(3)

      private val w0 = new PactInteger()
      private val w1 = new PactDouble()
      private val w2 = new PactDouble()
      private val w3 = new PactDouble()

      override def serialize(item: (Int, Point), record: PactRecord) = {
        val (v0, Point(v1, v2, v3)) = item

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

        if (ix3 >= 0) {
          w3.setValue(v3)
          record.setField(ix3, w3)
        }
      }

      override def deserialize(record: PactRecord): (Int, Point) = {
        var v0: Int = 0
        var v1: Double = 0
        var v2: Double = 0
        var v3: Double = 0

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

        if (ix3 >= 0) {
          record.getFieldInto(ix3, w3)
          v3 = w3.getValue()
        }

        (v0, Point(v1, v2, v3))
      }
    }
  }

  implicit val intDistanceSerializer: UDT[(Int, Distance)] = new UDT[(Int, Distance)] {

    override val fieldCount = 6

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Distance)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)
      private val ix3 = indexMap(3)
      private val ix4 = indexMap(4)
      private val ix5 = indexMap(5)

      private val w0 = new PactInteger()
      private val w1 = new PactDouble()
      private val w2 = new PactDouble()
      private val w3 = new PactDouble()
      private val w4 = new PactInteger()
      private val w5 = new PactDouble()

      override def serialize(item: (Int, Distance), record: PactRecord) = {
        val (v0, Distance(Point(v1, v2, v3), v4, v5)) = item

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

        if (ix3 >= 0) {
          w3.setValue(v3)
          record.setField(ix3, w3)
        }

        if (ix4 >= 0) {
          w4.setValue(v4)
          record.setField(ix4, w4)
        }

        if (ix5 >= 0) {
          w5.setValue(v5)
          record.setField(ix5, w5)
        }
      }

      override def deserialize(record: PactRecord): (Int, Distance) = {
        var v0: Int = 0
        var v1: Double = 0
        var v2: Double = 0
        var v3: Double = 0
        var v4: Int = 0
        var v5: Double = 0

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

        if (ix3 >= 0) {
          record.getFieldInto(ix3, w3)
          v3 = w3.getValue()
        }

        if (ix4 >= 0) {
          record.getFieldInto(ix4, w4)
          v4 = w4.getValue()
        }

        if (ix5 >= 0) {
          record.getFieldInto(ix5, w5)
          v5 = w5.getValue()
        }

        (v0, Distance(Point(v1, v2, v3), v4, v5))
      }
    }
  }

  implicit val intPointSumSerializer: UDT[(Int, PointSum)] = new UDT[(Int, PointSum)] {

    override val fieldCount = 5

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, PointSum)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)
      private val ix3 = indexMap(3)
      private val ix4 = indexMap(4)

      private val w0 = new PactInteger()
      private val w1 = new PactInteger()
      private val w2 = new PactDouble()
      private val w3 = new PactDouble()
      private val w4 = new PactDouble()

      override def serialize(item: (Int, PointSum), record: PactRecord) = {
        val (v0, PointSum(v1, Point(v2, v3, v4))) = item

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

        if (ix3 >= 0) {
          w3.setValue(v3)
          record.setField(ix3, w3)
        }

        if (ix4 >= 0) {
          w4.setValue(v4)
          record.setField(ix4, w4)
        }
      }

      override def deserialize(record: PactRecord): (Int, PointSum) = {
        var v0: Int = 0
        var v1: Int = 0
        var v2: Double = 0
        var v3: Double = 0
        var v4: Double = 0

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

        if (ix3 >= 0) {
          record.getFieldInto(ix3, w3)
          v3 = w3.getValue()
        }

        if (ix4 >= 0) {
          record.getFieldInto(ix4, w4)
          v4 = w4.getValue()
        }

        (v0, PointSum(v1, Point(v2, v3, v4)))
      }
    }
  }
}