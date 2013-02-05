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

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class KMeansMutableDescriptor extends PactDescriptor[KMeansMutable] {
  override val name = "KMeans Iteration (Mutable)"
  override val parameters = "[-numIterations <int:2>] -dataPoints <file> -clusterCenters <file> -output <file>"

  override def createInstance(args: Pact4sArgs) = new KMeansMutable(args("numIterations", "2").toInt, args("dataPoints"), args("clusterCenters"), args("output"))
}

class KMeansMutable(numIterations: Int, dataPointInput: String, clusterInput: String, clusterOutput: String) extends PactProgram {

  val dataPoints = new DataSource(dataPointInput, DelimetedDataSourceFormat(parseInput))
  val clusterPoints = new DataSource(clusterInput, DelimetedDataSourceFormat(parseInput))
  val newClusterPoints = new DataSink(clusterOutput, DelimetedDataSinkFormat(formatOutput))

  val finalCenters = computeNewCenters repeat (n = 3, s0 = clusterPoints)

  override def outputs = newClusterPoints <~ finalCenters

  def computeNewCenters = (centers: DataStream[MutableTuple2[Int, Point]]) => {

    val distances = dataPoints cross centers map computeDistance
    val nearestCenters = distances groupBy { case MutableTuple2(pid, _) => pid } combine { ds => ds.minBy(_._2.distance) } map asPointSum
    val newCenters = nearestCenters groupBy { case MutableTuple2(cid, _) => cid } combine sumPointSums map { case MutableTuple2(cid, pSum) => MutableTuple2(cid,  pSum.toPoint()) }

    distances.left neglects { case MutableTuple2(pid, _) => pid }
    distances.left preserves { case MutableTuple2(pid, p) => (pid, p) } as { case MutableTuple2(pid, dist) => (pid, dist.dataPoint) }
    distances.right neglects { case MutableTuple2(cid, _) => cid }
    distances.right preserves { case MutableTuple2(cid, _) => cid } as { case MutableTuple2(_, dist) => dist.clusterId }

    nearestCenters neglects { case MutableTuple2(pid, _) => pid }

    newCenters neglects { case MutableTuple2(cid, _) => cid }
    newCenters.preserves { case MutableTuple2(cid, _) => cid } as { case MutableTuple2(cid, _) => cid }

    distances.avgBytesPerRecord(48)
    nearestCenters.avgBytesPerRecord(40)
    newCenters.avgBytesPerRecord(36)

    newCenters
  }

  def computeDistance = (p: MutableTuple2[Int, Point], c: MutableTuple2[Int, Point]) => {
    val (MutableTuple2(pid, dataPoint), MutableTuple2(cid, clusterPoint)) = (p, c)
    val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)

    MutableTuple2(pid, Distance(dataPoint, cid, distToCluster))
  }

  def asPointSum = (idAndDist: MutableTuple2[Int, Distance]) => MutableTuple2(idAndDist._2.clusterId, PointSum(1, idAndDist._2.dataPoint))

  def sumPointSums = (dataPoints: Iterator[MutableTuple2[Int, PointSum]]) => dataPoints.reduce { (z, v) => z.copy(_2 = z._2 + v._2) }

  case class MutableTuple2[A, B](var _1: A, var _2: B)
  
  case class Point(var x: Double, var y: Double) {
    def computeEuclidianDistance(other: Point) = other match {
      case Point(x2, y2) => math.sqrt(math.pow(x - x2, 2) + math.pow(y - y2, 2))
    }
  }

  case class Distance(var dataPoint: Point, var clusterId: Int, var distance: Double)

  case class PointSum(var count: Int, var pointSum: Point) {
    def +(that: PointSum) = that match {
      case PointSum(c, Point(x, y)) => PointSum(count + c, Point(x + pointSum.x, y + pointSum.y))
    }

    def toPoint() = Point(round(pointSum.x / count), round(pointSum.y / count))

    // Rounding ensures that we get the same results in a multi-iteration run
    // as we do in successive single-iteration runs, since the output format
    // only contains two decimal places.
    private def round(d: Double) = math.round(d * 100.0) / 100.0;
  }
  
  def parseInput = (line: String) => {
    val PointInputPattern = """(\d+)\|(\d+\.\d+)\|(\d+\.\d+)\|""".r
    val PointInputPattern(id, x, y) = line
    MutableTuple2(id.toInt, Point(x.toDouble, y.toDouble))
  }

  def formatOutput = (center: MutableTuple2[Int, Point]) => "%d|%.2f|%.2f|".format(center._1, center._2.x, center._2.y)

  clusterPoints.degreeOfParallelism(1)
}

