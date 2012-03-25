package eu.stratosphere.pact4s.example.datamining

import scala.collection.JavaConversions._
import scala.math._

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.util.Iterator

import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.pact.common.contract.FileDataSinkContract
import eu.stratosphere.pact.common.contract.FileDataSourceContract
import eu.stratosphere.pact.common.contract.OutputContract
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.io.TextInputFormat
import eu.stratosphere.pact.common.io.TextOutputFormat
import eu.stratosphere.pact.common.plan.Plan
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.pact.common.stub.Collector
import eu.stratosphere.pact.common.stub.CrossStub
import eu.stratosphere.pact.common.stub.ReduceStub
import eu.stratosphere.pact.common.`type`.KeyValuePair
import eu.stratosphere.pact.common.`type`.Value
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

    val dataPoints = new FileDataSourceContract(classOf[PointInFormat], dataPointInput, "Data Points")
    dataPoints.setParameter(TextInputFormat.RECORD_DELIMITER, delimeter);
    dataPoints.setDegreeOfParallelism(numSubTasks);
    dataPoints.setOutputContract(classOf[OutputContract.UniqueKey])

    val clusterPoints = new FileDataSourceContract(classOf[PointInFormat], clusterInput, "Centers")
    clusterPoints.setParameter(TextInputFormat.RECORD_DELIMITER, delimeter);
    clusterPoints.setDegreeOfParallelism(1);
    clusterPoints.setOutputContract(classOf[OutputContract.UniqueKey])

    val computeDistance = new CrossContract(classOf[ComputeDistance], "Compute Distances")
    computeDistance.setDegreeOfParallelism(numSubTasks);
    computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

    val findNearestClusterCenters = new ReduceContract(classOf[FindNearestCenter], "Find Nearest Centers")
    findNearestClusterCenters.setDegreeOfParallelism(numSubTasks);
    findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

    val recomputeClusterCenter = new ReduceContract(classOf[RecomputeClusterCenter], "Recompute Center Positions")
    recomputeClusterCenter.setDegreeOfParallelism(numSubTasks);
    recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);

    val newClusterPoints = new FileDataSinkContract(classOf[PointOutFormat], output, "New Centers")
    newClusterPoints.setDegreeOfParallelism(numSubTasks);

    computeDistance.setFirstInput(dataPoints)
    computeDistance.setSecondInput(clusterPoints)
    findNearestClusterCenters.setInput(computeDistance)
    recomputeClusterCenter.setInput(findNearestClusterCenters)
    newClusterPoints.setInput(recomputeClusterCenter)

    new Plan(newClusterPoints, "KMeans Iteration")
  }

  override def getDescription() = "Parameters: [noSubStasks] [dataPoints] [clusterCenters] [output]"
}

object KMeansScalaPlain {

  @OutputContract.SameKeyFirst
  class ComputeDistance extends CrossStub[PactInteger, Point, PactInteger, Point, PactInteger, Distance] {

    override def cross(pid: PactInteger, dataPoint: Point, cid: PactInteger, clusterPoint: Point, out: Collector[PactInteger, Distance]) = {
      val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)
      out.collect(pid, Distance(dataPoint, cid.getValue(), distToCluster))
    }
  }

  @ReduceContract.Combinable
  class FindNearestCenter extends ReduceStub[PactInteger, Distance, PactInteger, PointSum] {

    override def reduce(pid: PactInteger, distancesList: Iterator[Distance], out: Collector[PactInteger, PointSum]) = {
      val Distance(dataPoint, clusterId, _) = getNearestCluster(distancesList)
      out.collect(new PactInteger(clusterId), PointSum(1, dataPoint))
    }

    override def combine(pid: PactInteger, distancesList: Iterator[Distance], out: Collector[PactInteger, Distance]) = {
      out.collect(pid, getNearestCluster(distancesList))
    }

    private def getNearestCluster(distancesList: Iterator[Distance]): Distance = distancesList.minBy(_.distance)
  }

  @OutputContract.SameKey @ReduceContract.Combinable
  class RecomputeClusterCenter extends ReduceStub[PactInteger, PointSum, PactInteger, Point] {

    override def reduce(cid: PactInteger, dataPoints: Iterator[PointSum], out: Collector[PactInteger, Point]) = {
      val pSum = sumPointSums(dataPoints)
      out.collect(cid, pSum.toPoint())
    }

    override def combine(cid: PactInteger, dataPoints: Iterator[PointSum], out: Collector[PactInteger, PointSum]) =
      out.collect(cid, sumPointSums(dataPoints))

    private def sumPointSums(dataPoints: Iterator[PointSum]): PointSum = {
      dataPoints.fold(PointSum(0, Point(0, 0, 0)))(_ + _)
    }
  }

  class PointInFormat extends TextInputFormat[PactInteger, Point] {

    val PointInputPattern = """(\d+)|(\d+\.\d+)|(\d+\.\d+)|(\d+\.\d+)|""".r

    override def readLine(pair: KeyValuePair[PactInteger, Point], line: Array[Byte]): Boolean = new String(line) match {
      case PointInputPattern(id, x, y, z) => {
        pair.setKey(new PactInteger(id.toInt))
        pair.setValue(Point(x.toDouble, y.toDouble, z.toDouble))
        true
      }
      case _ => false
    }
  }

  class PointOutFormat extends TextOutputFormat[PactInteger, Point] {

    override def writeLine(pair: KeyValuePair[PactInteger, Point]): Array[Byte] = {
      val id = pair.getKey().getValue()
      val Point(x, y, z) = pair.getValue()

      "%d|%.2f|%.2f|%.2f|\n".format(id, x, y, z).getBytes()
    }
  }

  case class Point(var x: Double, var y: Double, var z: Double) extends Value {

    def this() = this(0, 0, 0)

    def computeEuclidianDistance(other: Point) = other match {
      case Point(x2, y2, z2) => sqrt(pow(x - x2, 2) + pow(y - y2, 2) + pow(z - z2, 2))
    }

    override def read(in: DataInput) = {
      x = in.readDouble()
      y = in.readDouble()
      z = in.readDouble()
    }

    override def write(out: DataOutput) = {
      out.writeDouble(x)
      out.writeDouble(y)
      out.writeDouble(z)
    }
  }

  case class Distance(var dataPoint: Point, var clusterId: Int, var distance: Double) extends Value {

    def this() = this(null, 0, 0)

    override def read(in: DataInput) = {
      dataPoint = new Point()
      dataPoint.read(in)
      clusterId = in.readInt()
      distance = in.readDouble()
    }

    override def write(out: DataOutput) = {
      dataPoint.write(out)
      out.writeInt(clusterId)
      out.writeDouble(distance)
    }
  }

  case class PointSum(var count: Int, var pointSum: Point) extends Value {

    def this() = this(0, null)

    override def read(in: DataInput) = {
      count = in.readInt()
      pointSum = new Point()
      pointSum.read(in)
    }

    override def write(out: DataOutput) = {
      out.writeInt(count)
      pointSum.write(out)
    }

    def +(that: PointSum) = that match {
      case PointSum(c, Point(x, y, z)) => PointSum(count + c, Point(x + pointSum.x, y + pointSum.y, z + pointSum.z))
    }

    def toPoint() = Point(pointSum.x / count, pointSum.y / count, pointSum.z / count)
  }
}
