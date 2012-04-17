package eu.stratosphere.pact4s.example.datamining

import scala.math._

import eu.stratosphere.pact4s.common._

abstract class BatchGradientDescent(args: String*) extends PACTProgram {

  def gradient(w: Array[Double])(e: Array[Double]): Array[Double]

  val examples = new DataSource(params.examples, params.delimeter, readVector)
  val weights = new DataSource(params.weights, params.delimeter, readVector)
  val output = new DataSink(params.output, params.delimeter, formatOutput)

  // Warning: this isn't really a fixpoint calculation...
  // If the gradient descent doesn't converge, this won't terminate!
  val newWeights = fixpointIncremental(step, more)(weights, weights)

  override def outputs = output <~ newWeights

  def more(s: DataStream[Int, Array[Double]], ws: DataStream[Int, Array[Double]]) = ws.nonEmpty

  def step(s: DataStream[Int, Array[Double]], ws: DataStream[Int, Array[Double]]) = {

    val gradients = ws cross examples map { (wId: Int, w: Array[Double], eId: Int, e: Array[Double]) => gradient(w)(e) }
    val gradientSums = gradients combine { (wId: Int, ws: Iterable[Array[Double]]) => ws reduce add }
    val diffs = ws join gradientSums map { (wId: Int, wOld: Array[Double], wSum: Array[Double]) => (wOld, sub(wOld, mul(params.learningRate, wSum))) }

    val s1 = s cogroup diffs flatMap { (wId: Int, wOld: Iterable[Array[Double]], wNew: Iterable[(Array[Double], Array[Double])]) =>
      wNew.toSeq match {
        case Seq() => wOld
        case Seq((_, w)) => Seq(w)
      }
    }

    val ws1 = (diffs filter { (wId: Int, w: (Array[Double], Array[Double])) => abs(norm(w._1) - norm(w._2)) > params.eps }). 
    		         map { (wId: Int, w: (Array[Double], Array[Double])) => w._2 }
    
    (s1, ws1)
  }

  def add(v1: Array[Double], v2: Array[Double]): Array[Double] = (v1 zip v2) map { case (x1, x2) => x1 + x2 }
  def sub(v1: Array[Double], v2: Array[Double]): Array[Double] = (v1 zip v2) map { case (x1, x2) => x1 - x2 }
  def mul(x: Double, v: Array[Double]): Array[Double] = v map { x * _ }
  def norm(v: Array[Double]): Double = sqrt(v map { x => x * x } reduce { _ + _ })

  override def description = "Parameters: [noSubStasks] [eps] [learningRate] [examples] [weights] [output]"

  val params = new {
    val delimeter = "\n"
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val eps = if (args.length > 1) args(1).toDouble else 1
    val learningRate = if (args.length > 2) args(2).toDouble else 1
    val examples = if (args.length > 3) args(3) else ""
    val weights = if (args.length > 4) args(4) else ""
    val output = if (args.length > 5) args(5) else ""
  }

  override def getHints(item: Hintable) = item match {
    case examples() => Degree(params.numSubTasks) +: UniqueKey
    case weights() => Degree(params.numSubTasks) +: UniqueKey
    case output() => Degree(params.numSubTasks) +: UniqueKey
  }

  def readVector(line: String): Int --> Array[Double] = {

    val items = line.split(',')
    val id = items(0).toInt
    val vector = items.drop(1) map { _.toDouble }

    id --> vector
  }

  def formatOutput(id: Int, vector: Array[Double]) = "%s,%s".format(id, vector.mkString(","))
}