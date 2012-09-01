/**
 * *********************************************************************************************************************
 *
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
 *
 * ********************************************************************************************************************
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

class KMeans(numIterations: Int, dataPointInput: String, clusterInput: String, clusterOutput: String) extends PactProgram with KMeansGeneratedImplicits {

  val dataPoints = new DataSource(dataPointInput, DelimetedDataSourceFormat(parseInput))
  val clusterPoints = new DataSource(clusterInput, DelimetedDataSourceFormat(parseInput))
  val newClusterPoints = new DataSink(clusterOutput, DelimetedDataSinkFormat(formatOutput.tupled))

  val finalCenters = (computeNewCenters ^ numIterations)(clusterPoints)
  //val finalCenters = computeNewCenters ^ numIterations apply clusterPoints

  override def outputs = newClusterPoints <~ finalCenters

  def computeNewCenters = (centers: DataStream[(Int, Point)]) => {

    val distances = dataPoints cross centers map computeDistance
    val nearestCenters = distances groupBy { case (pid, _) => pid } combine { ds => ds.minBy(_._2.distance) } map asPointSum.tupled
    val newCenters = nearestCenters groupBy { case (cid, _) => cid } combine sumPointSums map { case (cid: Int, pSum: PointSum) => cid -> pSum.toPoint() }

    distances.hints = RecordSize(48) +: PactName("Distances")
    nearestCenters.hints = RecordSize(48) +: PactName("Nearest Centers")
    newCenters.hints = RecordSize(36) +: PactName("New Centers")

    newCenters
  }

  def computeDistance = (p: (Int, Point), c: (Int, Point)) => {
    val ((pid, dataPoint), (cid, clusterPoint)) = (p, c)
    val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)

    pid -> Distance(dataPoint, cid, distToCluster)
  }

  def asPointSum = (pid: Int, dist: Distance) => dist.clusterId -> PointSum(1, dist.dataPoint)

  def sumPointSums = (dataPoints: Iterator[(Int, PointSum)]) => dataPoints.reduce { (z, v) => z.copy(_2 = z._2 + v._2) }

  dataPoints.hints = PactName("Data Points")
  clusterPoints.hints = Degree(1) +: PactName("Cluster Points")
  newClusterPoints.hints = PactName("New Cluster Points")

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

trait KMeansGeneratedImplicits { this: KMeans =>

  import java.io.ObjectInputStream

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  /*
  implicit val intPointSerializer: UDT[(Int, Point)] = new UDT[(Int, Point)] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactDouble], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Point)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactDouble()
      @transient private var w2 = new PactDouble()

      override def serialize(item: (Int, Point), record: PactRecord) = {
        val (v0, Point(v1, v2)) = item

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

      override def deserialize(record: PactRecord): (Int, Point) = {
        var v0: Int = 0
        var v1: Double = 0
        var v2: Double = 0

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

        (v0, Point(v1, v2))
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactDouble()
        w2 = new PactDouble()
      }
    }
  }

  implicit val intDistanceSerializer: UDT[(Int, Distance)] = new UDT[(Int, Distance)] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactDouble], classOf[PactDouble], classOf[PactInteger], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Distance)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)
      private val ix3 = indexMap(3)
      private val ix4 = indexMap(4)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactDouble()
      @transient private var w2 = new PactDouble()
      @transient private var w3 = new PactInteger()
      @transient private var w4 = new PactDouble()

      override def serialize(item: (Int, Distance), record: PactRecord) = {
        val (v0, Distance(Point(v1, v2), v3, v4)) = item

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

      override def deserialize(record: PactRecord): (Int, Distance) = {
        var v0: Int = 0
        var v1: Double = 0
        var v2: Double = 0
        var v3: Int = 0
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

        (v0, Distance(Point(v1, v2), v3, v4))
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactDouble()
        w2 = new PactDouble()
        w3 = new PactInteger()
        w4 = new PactDouble()
      }
    }
  }

  implicit val intPointSumSerializer: UDT[(Int, PointSum)] = new UDT[(Int, PointSum)] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactInteger], classOf[PactDouble], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, PointSum)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)
      private val ix3 = indexMap(3)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactInteger()
      @transient private var w2 = new PactDouble()
      @transient private var w3 = new PactDouble()

      override def serialize(item: (Int, PointSum), record: PactRecord) = {
        val (v0, PointSum(v1, Point(v2, v3))) = item

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

      override def deserialize(record: PactRecord): (Int, PointSum) = {
        var v0: Int = 0
        var v1: Int = 0
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

        (v0, PointSum(v1, Point(v2, v3)))
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactInteger()
        w2 = new PactDouble()
        w3 = new PactDouble()
      }
    }
  }
  */

  /*
  implicit def udf1(fun: Function1[Iterator[(Int, Distance)], (Int, Distance)]): UDF1Code[Function1[Iterator[(Int, Distance)], (Int, Distance)]] = AnalyzedUDF1.defaultIterT(fun)
  implicit def udf2(fun: Function1[(Int, Distance), (Int, PointSum)]): UDF1Code[Function1[(Int, Distance), (Int, PointSum)]] = AnalyzedUDF1.default(fun)
  implicit def udf3(fun: Function1[Iterator[(Int, PointSum)], (Int, PointSum)]): UDF1Code[Function1[Iterator[(Int, PointSum)], (Int, PointSum)]] = AnalyzedUDF1.defaultIterT(fun)
  implicit def udf4(fun: Function1[(Int, PointSum), (Int, Point)]): UDF1Code[Function1[(Int, PointSum), (Int, Point)]] = AnalyzedUDF1.default(fun)
  implicit def udf5(fun: Function2[(Int, Point), (Int, Point), (Int, Distance)]): UDF2Code[Function2[(Int, Point), (Int, Point), (Int, Distance)]] = AnalyzedUDF2.default(fun)
  implicit def udfOut(fun: Function1[(Int, Point), String]): UDF1Code[Function1[(Int, Point), String]] = AnalyzedUDF1.default(fun)
  */

  implicit def selNearestCenters(fun: Function1[(Int, Distance), Int]): FieldSelectorCode[Function1[(Int, Distance), Int]] = AnalyzedFieldSelector(fun, Set(0))
  implicit def selNewCenters(fun: Function1[(Int, PointSum), Int]): FieldSelectorCode[Function1[(Int, PointSum), Int]] = AnalyzedFieldSelector(fun, Set(0))
}

