/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.pact4s.examples.datamining

import scala.math._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class KMeansDescriptor extends PactDescriptor[KMeans] {
  override val name = "KMeans Iteration"
  override val description = "Parameters: [numSubTasks] [numIterations] [dataPoints] [clusterCenters] [output]"
  override def getDefaultParallelism(args: Map[Int, String]) = args.getOrElse(0, "1").toInt

  override def createInstance(args: Map[Int, String]) = new KMeans(args.getOrElse(1, "2").toInt, args.getOrElse(2, "dataPoints"), args.getOrElse(3, "clusterCenters"), args.getOrElse(4, "output"))
}

class KMeans(numIterations: Int, dataPointInput: String, clusterInput: String, clusterOutput: String) extends PactProgram {

  val dataPoints = new DataSource(dataPointInput, DelimetedDataSourceFormat(parseInput))
  val clusterPoints = new DataSource(clusterInput, DelimetedDataSourceFormat(parseInput))
  val newClusterPoints = new DataSink(clusterOutput, DelimetedDataSinkFormat(formatOutput.tupled))

  val finalCenters = (computeNewCenters ^ numIterations)(clusterPoints)

  override def outputs = newClusterPoints <~ finalCenters

  def computeNewCenters = (centers: DataStream[(Int, Point)]) => {

    val distances = dataPoints cross centers map computeDistance
    val nearestCenters = distances groupBy { case (pid, _) => pid } combine { ds => ds.minBy(_._2.distance) } map asPointSum.tupled    
    val newCenters = nearestCenters groupBy { case (cid, _) => cid } combine sumPointSums map { case (cid: Int, pSum: PointSum) => cid -> pSum.toPoint() }

    distances.left ignores { case (pid, _) => pid }
    distances.left preserves { dp => dp } as { case (pid, dist) => (pid, dist.dataPoint) }
    distances.right ignores { case (cid, _) => cid }
    distances.right preserves { case (cid, _) => cid } as { case (_, dist) => dist.clusterId }
    distances.avgBytesPerRecord(48)
    
    nearestCenters.ignores { case (pid, _) => pid }
    nearestCenters.avgBytesPerRecord(40)
    
    newCenters.ignores { case (cid, _) => cid }
    newCenters.preserves { case (cid, _) => cid } as { case (cid, _) => cid }
    newCenters.avgBytesPerRecord(36)

    newCenters
  }

  def computeDistance = (p: (Int, Point), c: (Int, Point)) => {
    val ((pid, dataPoint), (cid, clusterPoint)) = (p, c)
    val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)

    pid -> Distance(dataPoint, cid, distToCluster)
  }

  def asPointSum = (pid: Int, dist: Distance) => dist.clusterId -> PointSum(1, dist.dataPoint)

  def sumPointSums = (dataPoints: Iterator[(Int, PointSum)]) => dataPoints.reduce { (z, v) => z.copy(_2 = z._2 + v._2) }

  clusterPoints.degreeOfParallelism(1)

  val PointInputPattern = """(\d+)\|(\d+\.\d+)\|(\d+\.\d+)\|""".r

  def parseInput = (line: String) => line match {
    case PointInputPattern(id, x, y) => id.toInt -> Point(x.toDouble, y.toDouble)
  }

  def formatOutput = (cid: Int, p: Point) => "%d|%.2f|%.2f|".format(cid, p.x, p.y)

  case class Point(x: Double, y: Double) {
    def computeEuclidianDistance(other: Point) = other match {
      case Point(x2, y2) => sqrt(pow(x - x2, 2) + pow(y - y2, 2))
    }
  }

  case class Distance(dataPoint: Point, clusterId: Int, distance: Double)

  case class PointSum(count: Int, pointSum: Point) {
    def +(that: PointSum) = that match {
      case PointSum(c, Point(x, y)) => PointSum(count + c, Point(x + pointSum.x, y + pointSum.y))
    }

    def toPoint() = Point(round(pointSum.x / count), round(pointSum.y / count))

    // Rounding ensures that we get the same results in a multi-iteration run
    // as we do in successive single-iteration runs, since the output format
    // only contains two decimal places.
    private def round(d: Double) = math.round(d * 100.0) / 100.0;
  }
}

