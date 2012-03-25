package eu.stratosphere.pact4s.example.datamining

import scala.math._
import eu.stratosphere.pact4s.common._

class KMeansScalaDSL(args: String*) extends PACTProgram {

  val dataPoints       = new DataSource(params.dataPointInput, params.delimeter, parseInput)
  val clusterPoints    = new DataSource(params.clusterInput, params.delimeter, parseInput)
  val newClusterPoints = new DataSink(params.output, params.delimeter, formatOutput)

  val distances        = dataPoints cross clusterPoints map computeDistance _
  val nearestCenters   = distances combine { (_, ds) => ds.minBy(_.distance) } map asPointSum _
  val newCenters       = nearestCenters combine sumPointSums map { (cid: Int, pSum: PointSum) => pSum.toPoint() }

  override def outputs = newClusterPoints <~ newCenters

  def computeDistance(pid: Int, dataPoint: Point, cid: Int, clusterPoint: Point): Distance = {
    val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)
    Distance(dataPoint, cid, distToCluster)
  }

  def asPointSum(pid: Int, distance: Distance): Int --> PointSum = distance match {
    case Distance(dataPoint, clusterId, _) => clusterId --> PointSum(1, dataPoint)
  }

  def sumPointSums(cid: Int, dataPoints: Iterable[PointSum]): PointSum = {
    dataPoints.fold(PointSum(0, Point(0, 0, 0)))(_ + _)
  }

  val PointInputPattern = """(\d+)|(\d+\.\d+)|(\d+\.\d+)|(\d+\.\d+)|""".r

  def parseInput(line: String): Int --> Point = line match {
    case PointInputPattern(id, x, y, z) => id.toInt --> Point(x.toDouble, y.toDouble, z.toDouble)
  }

  def formatOutput(cid: Int, dataPoint: Point): String = dataPoint match {
    case Point(x, y, z) => "%d|%.2f|%.2f|%.2f|".format(cid, x, y, z)
  }
  
  override def name = "KMeans Iteration"
  override def description = "Parameters: [noSubStasks] [dataPoints] [clusterCenters] [output]"

  val params = new {
    val delimeter      = "\n"
    val numSubTasks    = if (args.length > 0) args(0).toInt else 1
    val dataPointInput = if (args.length > 1) args(1) else ""
    val clusterInput   = if (args.length > 2) args(2) else ""
    val output         = if (args.length > 3) args(3) else ""
  }

  override def getHints(item: Hintable) = item match {
    case dataPoints()       => UniqueKey +: Degree(params.numSubTasks)
    case clusterPoints()    => UniqueKey +: Degree(1)
    case newClusterPoints() => Degree(params.numSubTasks)
    case distances()        => Degree(params.numSubTasks) +: AvgRecordSize(48)
    case nearestCenters()   => Degree(params.numSubTasks) +: AvgRecordSize(48)
    case newCenters()       => Degree(params.numSubTasks) +: AvgRecordSize(36)
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
