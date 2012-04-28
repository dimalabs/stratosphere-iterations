package eu.stratosphere.pact4s.examples.datamining

import scala.collection.JavaConversions._
import scala.math._

import java.util.Iterator

import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.FileDataSource
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.plan.Plan
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.common.stubs.CrossStub
import eu.stratosphere.pact.common.stubs.ReduceStub
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactDouble

class KMeansScalaPlain extends PlanAssembler with PlanAssemblerDescription {

  import KMeansScalaPlain._

  override def getPlan(args: String*): Plan = {
    val delimeter = "\n"
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val dataPointInput = if (args.length > 1) args(1) else ""
    val clusterInput = if (args.length > 2) args(2) else ""
    val output = if (args.length > 3) args(3) else ""

    val dataPoints = new FileDataSource(classOf[PointInFormat], dataPointInput, "Data Points")
    dataPoints.setParameter(DelimitedInputFormat.RECORD_DELIMITER, delimeter);

    val clusterPoints = new FileDataSource(classOf[PointInFormat], clusterInput, "Centers")
    clusterPoints.setParameter(DelimitedInputFormat.RECORD_DELIMITER, delimeter);
    clusterPoints.setDegreeOfParallelism(1);

    val computeDistance = new CrossContract(classOf[ComputeDistance], dataPoints, clusterPoints, "Compute Distances")
    computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

    val findNearestClusterCenters = new ReduceContract(classOf[FindNearestCenter], classOf[PactInteger], 0, computeDistance, "Find Nearest Centers")
    findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

    val recomputeClusterCenter = new ReduceContract(classOf[RecomputeClusterCenter], classOf[PactInteger], 0, findNearestClusterCenters, "Recompute Center Positions")
    recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);

    val newClusterPoints = new FileDataSink(classOf[PointOutFormat], output, recomputeClusterCenter, "New Center Positions")

    val plan = new Plan(newClusterPoints, "KMeans Iteration")
    plan.setDefaultParallelism(numSubTasks)

    plan
  }

  override def getDescription() = "Parameters: [noSubStasks] [dataPoints] [clusterCenters] [output]"
}

object KMeansScalaPlain {

  class ComputeDistance extends CrossStub {

    // (Int, Point) ** (Int, Point) -> (Int, Distance)
    override def cross(dataPointRecord: PactRecord, clusterCenterRecord: PactRecord, out: Collector) = {
      val dataPoint = Point.deserialize(1, dataPointRecord)
      val clusterId = clusterCenterRecord.getField(0, classOf[PactInteger])
      val clusterPoint = Point.deserialize(1, clusterCenterRecord)

      val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)

      dataPointRecord.setField(1 + Point.width, clusterId)
      dataPointRecord.setField(2 + Point.width, distToCluster)

      out.collect(dataPointRecord)
    }
  }

  @ReduceContract.Combinable
  class FindNearestCenter extends ReduceStub {

    // [(Int, Distance)] -> (Int, PointSum)
    override def reduce(distanceRecords: Iterator[PactRecord], out: Collector) = {
      val Distance(dataPoint, clusterId, _) = distanceRecords map { Distance.deserialize(1, _) } minBy { _.distance }

      val res = new PactRecord(2 + Point.width)
      res.setField(0, clusterId)
      res.setField(1, 1)
      dataPoint.serialize(2, res)

      out.collect(res)
    }

    // [(Int, Distance)] -> (Int, Distance)
    override def combine(distanceRecords: Iterator[PactRecord], out: Collector) = {
      val min = distanceRecords minBy { _.getField[PactDouble](3, classOf[PactDouble]).getValue() }

      out.collect(min)
    }
  }

  @ReduceContract.Combinable
  class RecomputeClusterCenter extends ReduceStub {

    // [(Int, PointSum)] -> (Int, Point)
    override def reduce(pointSumRecords: Iterator[PactRecord], out: Collector) = {
      val res = pointSumRecords.next()
      val pSum = sumPointSums(pointSumRecords)

      res.setNumFields(1 + Point.width)
      pSum.toPoint().serialize(1, res)
      out.collect(res)
    }

    // [(Int, PointSum)] -> (Int, PointSum)
    override def combine(pointSumRecords: Iterator[PactRecord], out: Collector) = {
      val res = pointSumRecords.next()
      val pSum = sumPointSums(pointSumRecords)

      pSum.serialize(1, res)
      out.collect(res)
    }

    private def sumPointSums(pointSumRecords: Iterator[PactRecord]): PointSum = {
      val dataPoints = pointSumRecords map { PointSum.deserialize(1, _) }
      dataPoints.fold(PointSum(0, Point(0, 0)))(_ + _)
    }
  }

  class PointInFormat extends DelimitedInputFormat {

    val PointInputPattern = """(\d+)\|(\d+\.\d+)\|(\d+\.\d+)\|""".r

    override def readRecord(record: PactRecord, line: Array[Byte], numBytes: Int): Boolean = {
      val PointInputPattern(id, x, y) = new String(line)
      record.setNumFields(3)
      record.setField(0, id.toInt)
      record.setField(1, x.toDouble)
      record.setField(2, y.toDouble)
      true
    }
  }

  class PointOutFormat extends DelimitedOutputFormat {

    override def serializeRecord(record: PactRecord, target: Array[Byte]): Int = {
      val id = record.getField[PactInteger](0, classOf[PactInteger])
      val Point(x, y) = Point.deserialize(1, record)

      val byteString = "%d|%.2f|%.2f|".format(id, x, y).getBytes()

      if (byteString.length <= target.length) {
        System.arraycopy(byteString, 0, target, 0, byteString.length);
        byteString.length;
      } else {
        -byteString.length;
      }
    }
  }

  case class Point(val x: Double, val y: Double) {

    def computeEuclidianDistance(other: Point) = other match {
      case Point(x2, y2) => sqrt(pow(x - x2, 2) + pow(y - y2, 2))
    }

    def serialize(offset: Int, r: PactRecord) = {
      r.setField(offset, x)
      r.setField(offset + 1, y)
    }
  }

  object Point {
    val width = 2

    def deserialize(offset: Int, r: PactRecord): Point = {
      val x = r.getField[PactDouble](offset, classOf[PactDouble])
      val y = r.getField[PactDouble](offset + 1, classOf[PactDouble])
      Point(x, y)
    }
  }

  case class Distance(val dataPoint: Point, val clusterId: Int, val distance: Double) {

    def serialize(offset: Int, r: PactRecord) = {
      dataPoint.serialize(offset, r)
      r.setField(offset + Point.width, clusterId)
      r.setField(offset + Point.width + 1, distance)
    }
  }

  object Distance {
    val width = Point.width + 2

    def deserialize(offset: Int, r: PactRecord): Distance = {
      val p = Point.deserialize(offset, r)
      val c = r.getField[PactInteger](offset + Point.width, classOf[PactInteger])
      val d = r.getField[PactDouble](offset + Point.width + 1, classOf[PactDouble])
      Distance(p, c, d)
    }
  }

  case class PointSum(val count: Int, val pointSum: Point) {

    def +(that: PointSum) = that match {
      case PointSum(c, Point(x, y)) => PointSum(count + c, Point(x + pointSum.x, y + pointSum.y))
    }

    def toPoint() = Point(pointSum.x / count, pointSum.y / count)

    def serialize(offset: Int, r: PactRecord) = {
      r.setField(offset, count)
      pointSum.serialize(offset + 1, r)
    }
  }

  object PointSum {
    val width = Point.width + 1

    def deserialize(offset: Int, r: PactRecord): PointSum = {
      val c = r.getField[PactInteger](offset, classOf[PactInteger])
      val p = Point.deserialize(offset + 1, r)
      PointSum(c, p)
    }
  }

  implicit def int2PactInteger(x: Int): PactInteger = new PactInteger(x)
  implicit def pactInteger2Int(x: PactInteger): Int = x.getValue()
  implicit def double2PactDouble(x: Double): PactDouble = new PactDouble(x)
  implicit def pactDouble2Double(x: PactDouble): Double = x.getValue()
}
