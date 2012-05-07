package eu.stratosphere.pact4s.examples.datamining

import scala.math._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

trait BatchGradientDescentDescriptor[T <: BatchGradientDescent] extends PactDescriptor[T] {
  override val name = "Batch Gradient Descent"
  override val description = "Parameters: [noSubStasks] [eps] [eta] [lambda] [examples] [weights] [output]"
}

abstract class BatchGradientDescent(args: String*) extends PactProgram with BatchGradientDescentGeneratedImplicits {

  def computeGradient(ex: Array[Double], w: Array[Double]): (Double, Array[Double])

  val examples = new DataSource(params.examples, DelimetedDataSourceFormat(readVector _))
  val weights = new DataSource(params.weights, DelimetedDataSourceFormat(readVector _))
  val output = new DataSink(params.output, DelimetedDataSinkFormat(formatOutput _))

  val etaWeights = weights map { case (id, w) => (id, w, params.eta) }
  val newWeights = weights keyBy { _._1 } untilEmpty etaWeights iterate gradientDescent

  override def outputs = output <~ newWeights

  def gradientDescent(s: DataStream[(Int, Array[Double])], ws: DataStream[(Int, Array[Double], Double)]) = {

    val lossesAndGradients = ws cross examples map { case ((id, w, _), (_, ex)) => ValueAndGradient(id, computeGradient(ex, w)) }
    val lossAndGradientSums = lossesAndGradients groupBy { _.id } combine { _.reduce (_ + _) }
    val newWeights = ws join lossAndGradientSums on { _._1 } isEqualTo { _.id } map updateWeight

    // updated solution elements
    val s1 = newWeights map { case (wId, _, wNew, _) => (wId, wNew) }

    // new workset
    val ws1 = newWeights filter { case (_, delta, _, _) => delta > params.eps } map { case (wId, _, wNew, etaNew) => (wId, wNew, etaNew) }

    (s1, ws1)
  }

  def updateWeight(prev: (Int, Array[Double], Double), vg: ValueAndGradient) = {
    val (id, wOld, eta) = prev
    val ValueAndGradient(_, lossSum, gradSum) = vg

    val delta = lossSum + params.lambda * wOld.norm
    val wNew = (wOld + (gradSum * params.eta)) * (1 - eta * params.lambda)
    (id, delta, wNew, eta * 0.9)
  }

  override def defaultParallelism = params.numSubTasks

  val params = new {
    val numSubTasks = args(0).toInt
    val eps = args(1).toDouble
    val eta = args(2).toDouble
    val lambda = args(3).toDouble
    val examples = args(4)
    val weights = args(5)
    val output = args(6)
  }

  class WeightVector(vector: Array[Double]) {
    def +(that: Array[Double]): Array[Double] = (vector zip that) map { case (x1, x2) => x1 + x2 }
    def -(that: Array[Double]): Array[Double] = (vector zip that) map { case (x1, x2) => x1 - x2 }
    def *(x: Double): Array[Double] = vector map { x * _ }
    def norm: Double = sqrt(vector map { x => x * x } reduce { _ + _ })
  }

  implicit def array2WeightVector(vector: Array[Double]): WeightVector = new WeightVector(vector)

  case class ValueAndGradient(id: Int, value: Double, gradient: Array[Double]) {
    def +(that: ValueAndGradient) = ValueAndGradient(id, value + that.value, gradient + that.gradient)
  }

  object ValueAndGradient {
    def apply(id: Int, vg: (Double, Array[Double])) = new ValueAndGradient(id, vg._1, vg._2)
  }

  def readVector(line: String): (Int, Array[Double]) = {

    val items = line.split(',')
    val id = items(0).toInt
    val vector = items.drop(1) map { _.toDouble }

    id -> vector
  }

  def formatOutput(value: (Int, Array[Double])) = value match {
    case (id, vector) => "%s,%s".format(id, vector.mkString(","))
  }
}

trait BatchGradientDescentGeneratedImplicits { this: BatchGradientDescent =>

  import scala.collection.JavaConversions._

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val udf1: UDF1[Function1[(Int, Array[Double]), (Int, Array[Double], Double)]] = defaultUDF1[(Int, Array[Double]), (Int, Array[Double], Double)]
  implicit val udf2: UDF1[Function1[Iterator[ValueAndGradient], ValueAndGradient]] = defaultUDF1IterT[ValueAndGradient, ValueAndGradient]
  implicit val udf3: UDF1[Function1[(Int, Double, Array[Double], Double), (Int, Array[Double], Double)]] = defaultUDF1[(Int, Double, Array[Double], Double), (Int, Array[Double], Double)]
  implicit val udf4: UDF1[Function1[(Int, Double, Array[Double], Double), (Int, Array[Double])]] = defaultUDF1[(Int, Double, Array[Double], Double), (Int, Array[Double])]
  implicit val udf5: UDF2[Function2[(Int, Array[Double], Double), (Int, Array[Double]), ValueAndGradient]] = defaultUDF2[(Int, Array[Double], Double), (Int, Array[Double]), ValueAndGradient]
  implicit val udf6: UDF2[Function2[(Int, Array[Double], Double), ValueAndGradient, (Int, Double, Array[Double], Double)]] = defaultUDF2[(Int, Array[Double], Double), ValueAndGradient, (Int, Double, Array[Double], Double)]
  implicit val udf7: FieldSelector[Function1[(Int, Double, Array[Double], Double), Boolean]] = defaultFieldSelectorT[(Int, Double, Array[Double], Double), Boolean]

  implicit val selOutput: FieldSelector[Function1[(Int, Array[Double]), Unit]] = defaultFieldSelectorT[(Int, Array[Double]), Unit]
  implicit val selNewWeights: FieldSelector[Function1[(Int, Array[Double]), Int]] = getFieldSelector[(Int, Array[Double]), Int](0)
  implicit val selLossAndGradientSums: FieldSelector[Function1[ValueAndGradient, Int]] = getFieldSelector[ValueAndGradient, Int](0)
  implicit val selGdNewWeightsLeft: FieldSelector[Function1[(Int, Array[Double], Double), Int]] = getFieldSelector[(Int, Array[Double], Double), Int](0)

  implicit val intArrayDoubleUDT: UDT[(Int, Array[Double])] = new UDT[(Int, Array[Double])] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactList[PactDouble]])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Array[Double])] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)

      private val w0 = new PactInteger()
      private val w1 = new PactList[PactDouble]() {}

      override def serialize(item: (Int, Array[Double]), record: PactRecord) = {
        val (v0, v1) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.clear()
          w1.addAll(v1 map { new PactDouble(_) } toSeq)
          record.setField(ix1, w1)
        }
      }

      override def deserialize(record: PactRecord): (Int, Array[Double]) = {
        var v0: Int = 0
        var v1: Array[Double] = null

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1 map { _.getValue() } toArray
        }

        (v0, v1)
      }
    }
  }

  implicit val intArrayDoubleDoubleUDT: UDT[(Int, Array[Double], Double)] = new UDT[(Int, Array[Double], Double)] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactList[PactDouble]], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Array[Double], Double)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)

      private val w0 = new PactInteger()
      private val w1 = new PactList[PactDouble]() {}
      private val w2 = new PactDouble()

      override def serialize(item: (Int, Array[Double], Double), record: PactRecord) = {
        val (v0, v1, v2) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.clear()
          w1.addAll(v1 map { new PactDouble(_) } toSeq)
          record.setField(ix1, w1)
        }

        if (ix2 >= 0) {
          w2.setValue(v2)
          record.setField(ix2, w2)
        }
      }

      override def deserialize(record: PactRecord): (Int, Array[Double], Double) = {
        var v0: Int = 0
        var v1: Array[Double] = null
        var v2: Double = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1 map { _.getValue() } toArray
        }

        if (ix2 >= 0) {
          record.getFieldInto(ix2, w2)
          v2 = w2.getValue()
        }

        (v0, v1, v2)
      }
    }
  }

  implicit val intDoubleArrayDoubleDoubleUDT: UDT[(Int, Double, Array[Double], Double)] = new UDT[(Int, Double, Array[Double], Double)] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactDouble], classOf[PactList[PactDouble]], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[(Int, Double, Array[Double], Double)] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)
      private val ix3 = indexMap(3)

      private val w0 = new PactInteger()
      private val w1 = new PactDouble()
      private val w2 = new PactList[PactDouble]() {}
      private val w3 = new PactDouble()

      override def serialize(item: (Int, Double, Array[Double], Double), record: PactRecord) = {
        val (v0, v1, v2, v3) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }

        if (ix2 >= 0) {
          w2.clear()
          w2.addAll(v2 map { new PactDouble(_) } toSeq)
          record.setField(ix2, w2)
        }

        if (ix3 >= 0) {
          w3.setValue(v3)
          record.setField(ix3, w3)
        }
      }

      override def deserialize(record: PactRecord): (Int, Double, Array[Double], Double) = {
        var v0: Int = 0
        var v1: Double = 0
        var v2: Array[Double] = null
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
          v2 = w2 map { _.getValue() } toArray
        }

        if (ix3 >= 0) {
          record.getFieldInto(ix3, w3)
          v3 = w3.getValue()
        }

        (v0, v1, v2, v3)
      }
    }
  }

  implicit val valueAndGradientUDT: UDT[ValueAndGradient] = new UDT[ValueAndGradient] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactDouble], classOf[PactList[PactDouble]])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[ValueAndGradient] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)

      private val w0 = new PactInteger()
      private val w1 = new PactDouble()
      private val w2 = new PactList[PactDouble]() {}

      override def serialize(item: ValueAndGradient, record: PactRecord) = {
        val ValueAndGradient(v0, v1, v2) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }

        if (ix2 >= 0) {
          w2.clear()
          w2.addAll(v2 map { new PactDouble(_) } toSeq)
          record.setField(ix2, w2)
        }
      }

      override def deserialize(record: PactRecord): ValueAndGradient = {
        var v0: Int = 0
        var v1: Double = 0
        var v2: Array[Double] = null

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
          v2 = w2 map { _.getValue() } toArray
        }

        ValueAndGradient(v0, v1, v2)
      }
    }
  }
}