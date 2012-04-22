package eu.stratosphere.pact4s.example.datamining

import scala.math._
import eu.stratosphere.pact4s.common._

class KMeansScalaDSL(args: String*) extends PactProgram {

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

  dataPoints.hints = UniqueKey
  clusterPoints.hints = UniqueKey +: Degree(1)
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
