package eu.stratosphere.pact4s.example.wordcount

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._

class WordCount(args: String*) extends PactProgram with WordCountGeneratedImplicits {

  val input = new DataSource(params.input, readLine)
  val output = new DataSink(params.output, formatOutput)

  val words = input flatMap { line => line.toLowerCase().split("""\W+""") map { (_, 1) } }
  val counts = words groupBy { case (word, _) => word } combine { values => (values.head._1, values map { _._2 } sum) }

  override def outputs = output <~ counts

  override def name = "Word Count"
  override def description = "Parameters: [noSubStasks] [input] [output]"
  override def defaultParallelism = params.numSubTasks

  val params = new {
    val numSubTasks = args(0).toInt
    val input = args(1)
    val output = args(2)
  }

  def readLine(line: String): String = line

  def formatOutput(wordWithCount: (String, Int)): String = wordWithCount match {
    case (word, count) => "%s %d".format(word, count)
  }
}

trait WordCountGeneratedImplicits {

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val stringSerializer: PactSerializerFactory[String] = new PactSerializerFactory[String] {

    override val fieldCount = 1

    override def createInstance(indexMap: Array[Int]) = new PactSerializer {

      private val ix0 = indexMap(0)

      private val w0 = new PactString()

      override def serialize(item: String, record: PactRecord) = {

        if (ix0 >= 0) {
          w0.setValue(item)
          record.setField(ix0, w0)
        }
      }

      override def deserialize(record: PactRecord): String = {
        var v0: String = null

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        v0
      }
    }
  }

  implicit val stringIntSerializer: PactSerializerFactory[(String, Int)] = new PactSerializerFactory[(String, Int)] {

    override val fieldCount = 2

    override def createInstance(indexMap: Array[Int]) = new PactSerializer {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)

      private val w0 = new PactString()
      private val w1 = new PactInteger()

      override def serialize(item: (String, Int), record: PactRecord) = {
        val (v0, v1) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }
      }

      override def deserialize(record: PactRecord): (String, Int) = {
        var v0: String = null
        var v1: Int = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1.getValue()
        }

        (v0, v1)
      }
    }
  }
}