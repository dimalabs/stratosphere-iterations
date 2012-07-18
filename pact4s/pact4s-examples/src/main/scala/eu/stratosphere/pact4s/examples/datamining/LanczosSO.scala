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

class LanczosSODescriptor extends PactDescriptor[LanczosSO] {
  override val name = "LanczosSO"
  override val description = "Parameters: [numSubTasks] [k] [m] [ε] [A] [b] [λ] [Y]"
  override def getDefaultParallelism(args: Map[Int, String]) = args.getOrElse(0, "1").toInt

  override def createInstance(args: Map[Int, String]) = new LanczosSO(args.getOrElse(1, "10").toInt, args.getOrElse(2, "10").toInt, args.getOrElse(3, "0.05").toDouble,
    args.getOrElse(4, "A"), args.getOrElse(5, "b"), args.getOrElse(6, "λ"), args.getOrElse(7, "Y"))
}

class LanczosSO(k: Int, m: Int, ε: Double, inputA: String, inputB: String, outputLambda: String, outputY: String) extends PactProgram with LanczosSOGeneratedImplicits {

  val A = new DataSource(inputA, DelimetedDataSourceFormat(parseCell))
  val b = new DataSource(inputB, DelimetedDataSourceFormat(parseCell))
  val λsink = new DataSink(outputLambda, DelimetedDataSinkFormat(formatCell))
  val ysink = new DataSink(outputY, DelimetedDataSinkFormat(formatCell))

  val αʹβʹvʹ = mulVS(b, normV(b) map { 1 / _ }) flatMap { c =>
    val v0 = TaggedItem("v", Cell(0, c.col, 0))
    val v1 = TaggedItem("v", c.copy(row = 1))

    if (c.col == 0)
      Seq(TaggedItem("β", Cell(0, 0, 0)), v0, v1)
    else
      Seq(v0, v1)
  }

  val αβv = (stepI ^ m)(αʹβʹvʹ)

  val α = αβv filter { tc => tc.tag == "α" } map { _.item }
  val β = αβv filter { tc => tc.tag == "β" } map { _.item }
  val v = αβv filter { tc => tc.tag == "v" } map { _.item }

  val t = triDiag(α, β filter { _.col > 0 })
  val (q, d) = decompose(t)

  val λ = d filter { c => c.col == c.row } map { TaggedItem("", _) } groupBy { _.tag } reduce { eigenValues =>
    val highToLow = eigenValues.toSeq.sortBy(tc => abs(tc.item.value))(Ordering[Double].reverse)
    highToLow take (k) map { tc => tc.item.copy(row = 0) }
  } flatMap { c => c }

  val y = mulMM(v, q join λ on { _.col } isEqualTo { _.col } map { case (q, _) => q })

  override def outputs = Seq(λsink <~ λ, ysink <~ y)

  def stepI = (αʹβʹv: DataStream[TaggedItem[Cell]]) => {

    val i = 1 // need current iteration!

    val αʹ = αʹβʹv filter { tc => tc.tag == "α" } map { _.item }
    val βʹ = αʹβʹv filter { tc => tc.tag == "β" } map { _.item }
    val v = αʹβʹv filter { tc => tc.tag == "v" } map { _.item }

    val vᵢ = v filter { _.row == i }
    val vᵢᐨ = v filter { _.row == i - 1 }
    val βᵢᐨ = βʹ filter { c => c.col == i - 1 } map { _.value }

    val vᵢᐩʹʹʹ = mulMV(A, vᵢ)
    val αᵢ = dot(vᵢ, vᵢᐩʹʹʹ)
    val vᵢᐩʹʹ = sub(vᵢᐩʹʹʹ, sub(mulVS(vᵢᐨ, βᵢᐨ), mulVS(vᵢ, αᵢ)))
    val βᵢʹ = normV(vᵢᐩʹʹ)

    val α = union(αʹ, αᵢ map { Cell(0, i, _) })
    val t = triDiag(α, union(βʹ, βᵢʹ map { Cell(0, i, _) }))
    val (q, d) = decompose(t)

    val βᵢʹvᵢᐩʹʹ = union(βᵢʹ map { x => TaggedItem("β", Cell(0, 0, x)) }, vᵢᐩʹʹ map { TaggedItem("v", _) })

    val βᵢvᵢᐩʹ = (stepJ(i, v, q, normM(t)) ^ i)(βᵢʹvᵢᐩʹʹ)
    val βᵢ = βᵢvᵢᐩʹ filter { _.tag == "β" } map { _.item.value }
    val vᵢᐩʹ = βᵢvᵢᐩʹ filter { _.tag == "v" } map { _.item }

    val β = union(βʹ, βᵢ map { Cell(0, i, _) })
    val vᵢᐩ = mulVS(vᵢᐩʹ, βᵢ map { 1 / _ })
    val vᐩ = union(v, vᵢᐩ)

    union(union(α map { TaggedItem("α", _) }, β map { TaggedItem("β", _) }), vᐩ map { TaggedItem("v", _) })
  }

  def stepJ = (i: Int, v: DataStream[Cell], q: DataStream[Cell], normT: DataStream[Double]) => {

    val qvt = union(union(v map { TaggedItem("v", _) }, q map { TaggedItem("q", _) }), normT map { t => TaggedItem("t", Cell(0, 0, t)) })

    (βᵢʹvᵢᐩʹʹ: DataStream[TaggedItem[Cell]]) => {

      val j = 1 // need current iteration!

      (qvt map { (0, _) }) cogroup (βᵢʹvᵢᐩʹʹ map { (0, _) }) on { _._1 } isEqualTo { _._1 } flatMap {
        case (qvt, βᵢʹvᵢᐩʹʹ) => {

          val (tq, vt) = qvt.toSeq map { _._2 } partition { _.tag == "q" }
          val (tv, tt) = vt partition { _.tag == "v" }
          val q = tq map { _.item }
          val qij = q.find(c => c.row == i && c.col == j).get.value
          val normT = tt.head.item.value

          val (tβᵢʹ, tvᵢᐩʹʹ) = βᵢʹvᵢᐩʹʹ.toSeq map { _._2 } partition { _.tag == "β" }
          val βᵢʹ = tβᵢʹ.head.item.value

          if (βᵢʹ * abs(qij) <= sqrt(ε) * normT) {
            val v = tv map { _.item }
            val vᵢᐩʹʹ = tvᵢᐩʹʹ map { _.item }
            val r = mulMV(v, q filter { _.col == j })

            val vᵢᐩʹ = sub(vᵢᐩʹʹ, mulVS(r, dot(r, vᵢᐩʹʹ)))
            val βᵢ = Cell(0, i, norm(vᵢᐩʹ))

            TaggedItem("β", βᵢ) +: (vᵢᐩʹ map { c => TaggedItem("v", c) })

          } else {
            tβᵢʹ ++ tvᵢᐩʹʹ
          }
        }
      }
    }
  }

  def triDiag(α: DataStream[Cell], β: DataStream[Cell]) = {
    val diag = α map { c => c.copy(row = c.col) }
    val lower = β map { c => Cell(c.col, c.col - 1, c.value) }
    val upper = β map { c => Cell(c.col - 1, c.col, c.value) }
    union(diag, union(lower, upper))
  }

  def union[T: analyzer.UDT](x: DataStream[T], y: DataStream[T]) = {
    (x map { (0, _) }) cogroup (y map { (0, _) }) on { _._1 } isEqualTo { _._1 } flatMap { case (xs, ys) => (xs map { _._2 }) ++ (ys map { _._2 }) }
  }

  def normV(x: DataStream[Cell]): DataStream[Double] = throw new RuntimeException("Not implemented")
  def normM(x: DataStream[Cell]): DataStream[Double] = throw new RuntimeException("Not implemented")
  def dot(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Double] = throw new RuntimeException("Not implemented")
  def sub(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Cell] = throw new RuntimeException("Not implemented")
  def mulSS(x: DataStream[Double], y: DataStream[Double]): DataStream[Double] = throw new RuntimeException("Not implemented")
  def mulVS(x: DataStream[Cell], y: DataStream[Double]): DataStream[Cell] = throw new RuntimeException("Not implemented")
  def mulMM(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Cell] = throw new RuntimeException("Not implemented")
  def mulMV(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Cell] = throw new RuntimeException("Not implemented")
  def decompose(t: DataStream[Cell]): (DataStream[Cell], DataStream[Cell]) = throw new RuntimeException("Not implemented")

  def norm(x: Seq[Cell]): Double = throw new RuntimeException("Not implemented")
  def dot(x: Seq[Cell], y: Seq[Cell]): Double = throw new RuntimeException("Not implemented")
  def sub(x: Seq[Cell], y: Seq[Cell]): Seq[Cell] = throw new RuntimeException("Not implemented")
  def mulVS(x: Seq[Cell], y: Double): Seq[Cell] = throw new RuntimeException("Not implemented")
  def mulMV(x: Seq[Cell], y: Seq[Cell]): Seq[Cell] = throw new RuntimeException("Not implemented")

  case class Cell(row: Int, col: Int, value: Double)
  case class TaggedItem[T](tag: String, item: T)

  val CellInputPattern = """(\d+)\|(\d+)\|(\d+\.\d+)\|""".r

  def parseCell = (line: String) => line match {
    case CellInputPattern(row, col, value) => Cell(row.toInt, col.toInt, value.toDouble)
  }

  def formatCell = (cell: Cell) => "%d|%d|%.2f|".format(cell.row, cell.col, cell.value)
}

trait LanczosSOGeneratedImplicits { this: LanczosSO =>

  import java.io.ObjectInputStream

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val cellSerializer: UDT[Cell] = new UDT[Cell] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactInteger], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Cell] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactInteger()
      @transient private var w2 = new PactDouble()

      override def serialize(item: Cell, record: PactRecord) = {
        val Cell(v0, v1, v2) = item

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

      override def deserialize(record: PactRecord): Cell = {
        var v0: Int = 0
        var v1: Int = 0
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

        Cell(v0, v1, v2)
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactInteger()
        w2 = new PactDouble()
      }
    }
  }

  implicit def seqUDT[T: UDT]: UDT[Seq[T]] = {
    val seqBuilder = new collection.generic.CanBuildFrom[collection.GenTraversableOnce[T], T, Seq[T]] {
      override def apply() = new collection.mutable.ListBuffer[T]()
      override def apply(from: collection.GenTraversableOnce[T]) = new collection.mutable.ListBuffer[T]()
    }
    new udts.ListUDT[T, Seq]()(implicitly[UDT[T]], seqBuilder)
  }

  implicit def taggedItemSerializer[T: UDT]: UDT[TaggedItem[T]] = new UDT[TaggedItem[T]] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactString]) ++ implicitly[UDT[T]].fieldTypes

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[TaggedItem[T]] {

      private val ix0 = indexMap(0)
      private val indexMap1 = indexMap.drop(1)

      @transient private var w0 = new PactString()
      private val inner1 = implicitly[UDT[T]].createSerializer(indexMap1)

      override def serialize(item: TaggedItem[T], record: PactRecord) = {
        val TaggedItem(v0, v1) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        inner1.serialize(v1, record)
      }

      override def deserialize(record: PactRecord): TaggedItem[T] = {
        var v0: String = ""

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        val v1 = inner1.deserialize(record)

        TaggedItem(v0, v1)
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactString()
      }
    }
  }
}
