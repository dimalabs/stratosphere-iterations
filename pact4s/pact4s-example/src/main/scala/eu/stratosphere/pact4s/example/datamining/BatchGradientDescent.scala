package eu.stratosphere.pact4s.example.datamining

import scala.math._
import eu.stratosphere.pact4s.common._

abstract class BatchGradientDescent(args: String*) extends PactProgram {

  def computeGradient(ex: Array[Double], w: Array[Double]): (Double, Array[Double])

  val examples = new DataSource(params.examples, readVector)
  val weights = new DataSource(params.weights, readVector)
  val output = new DataSink(params.output, formatOutput)
  
  val etaWeights = weights map { case (id, w) => (id, w, params.eta) }

  val newWeights = weights untilEmpty etaWeights iterate gradientDescent
  
  override def outputs = output <~ newWeights
  
  def gradientDescent(s: DataStream[(Int, Array[Double])], ws: DataStream[(Int, Array[Double], Double)]) = {

    val lossesAndGradients = ws cross examples map { case ((id, w, _), (_, ex)) => ValueAndGradient(id, computeGradient(ex, w)) }
    val lossAndGradientSums = lossesAndGradients groupBy { _.id } combine sumLossesAndGradients
    val newWeights = ws join lossAndGradientSums on { _._1 } isEqualTo { _.id } map updateWeight

    // updated solution elements
    val s1 = newWeights map { case (wId, _, wNew, _) => (wId, wNew) }

    // new workset
    val ws1 = newWeights filter { case (_, delta, _, _) => delta > params.eps } map { case (wId, _, wNew, etaNew) => (wId, wNew, etaNew) }

    (s1, ws1)
  }
  
  def sumLossesAndGradients(values: Iterable[ValueAndGradient]) = {
    val id = values.head.id
    val lossSum = values map { _.value } sum
    val gradSum = values map { _.gradient } reduce (_ + _)
    ValueAndGradient(id, lossSum, gradSum)
  }
  
  def updateWeight(prev: (Int, Array[Double], Double), vg: ValueAndGradient) = prev match {
    case (id, wOld, eta) => vg match {
      case ValueAndGradient(_, lossSum, gradSum) => {
        val delta = lossSum + params.lambda * wOld.norm
        val wNew = (wOld + (gradSum * params.eta)) * (1 - eta * params.lambda)
        (id, delta, wNew, eta * 0.9)
      }
    }
  }

  override def description = "Parameters: [noSubStasks] [eps] [eta] [lambda] [examples] [weights] [output]"
  override def defaultParallelism = params.numSubTasks

  examples.hints = UniqueKey
  weights.hints = UniqueKey
  output.hints = UniqueKey

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
  
  case class ValueAndGradient(id: Int, value: Double, gradient: Array[Double])
  
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