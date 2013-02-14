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

package eu.stratosphere.pact4s.tests.perf.mutable

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class WordCountDescriptor extends PactDescriptor[WordCount] {
  override val name = "Word Count (Mutable)"
  override val parameters = "-input <file> -output <file>"

  override def createInstance(args: Pact4sArgs) = new WordCount(args("input"), args("output"))
}

class WordCount(textInput: String, wordsOutput: String) extends PactProgram {

  import WordCount._

  val input = new DataSource(textInput, new DenotationDataSourceFormat("ASCII"))
  val output = new DataSink(wordsOutput, new RecordDataSinkFormat[WordWithCount]("\n", " ", true))

  val words = input flatMap { _.toAlphaNumeric.toLowerCase.tokens map { WordWithCount(_, 1) } }

  val counts = words groupBy { _.word } combine { wcs =>

    var wc: WordWithCount = null
    var count = 0

    while (wcs.hasNext) {
      wc = wcs.next
      count += wc.count
    }

    WordWithCount(wc.word, count)
  }

  override def outputs = output <~ counts

  counts neglects { wc => wc.word }
  counts preserves { wc => wc.word } as { wc => wc.word }
}

object WordCount {

  case class WordWithCount(var word: Denotation, var count: Int)

  import eu.stratosphere.pact4s.common.analysis.UDT
  import eu.stratosphere.pact4s.common.analysis.UDTSerializer

  import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
  import eu.stratosphere.pact.common.`type`.base.PactString
  import eu.stratosphere.pact.common.`type`.PactRecord
  import eu.stratosphere.pact.common.io.TextInputFormat
  import eu.stratosphere.nephele.configuration.Configuration

  case class Denotation(var chars: Array[Char], var start: Int, var length: Int) {

    def toAlphaNumeric() = {
      var pos = start
      val limit = length

      while (pos < length) {
        val c = chars(pos)
        if (!(Character.isLetter(c) || Character.isDigit(c) || c == '_')) {
          chars(pos) = ' '
        }
        pos = pos + 1
      }

      this
    }

    def toLowerCase() = {
      var pos = start
      val limit = length

      while (pos < length) {
        chars(pos) = Character.toLowerCase(chars(pos))
        pos = pos + 1
      }

      this
    }

    def tokens = new Traversable[Denotation] {
      def foreach[T](f: Denotation => T): Unit = {

        var pos = start
        val limit = length

        while (pos < limit) {

          while (pos < limit && Character.isWhitespace(chars(pos))) {
            pos = pos + 1
          }

          if (pos < limit) {

            val begin = pos

            while (pos < limit && !Character.isWhitespace(chars(pos))) {
              pos = pos + 1
            }

            f(Denotation(chars, begin, pos - begin))
          }
        }
      }
    }

    override def toString() = new String(chars, start, length)
  }

  implicit def denotationUDT = new UDT[Denotation] {

    override val fieldTypes = Array[Class[_ <: PactValue]](classOf[PactString])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Denotation] {

      private val index = indexMap(0)

      @transient private var pactField = new PactString()
      @transient private var instance = new Denotation(null, 0, 0)

      override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
        case Seq() => List(index)
        case _ => invalidSelection(selection)
      }

      override def serialize(item: Denotation, record: PactRecord) = {
        if (index >= 0) {
          pactField.setValue(item.chars, item.start, item.length)
          record.setField(index, pactField)
        }
      }

      override def deserializeRecyclingOff(record: PactRecord): Denotation = {
        if (index >= 0) {
          val field = record.getField(index, pactField)
          val length = field.length
          val chars = new Array[Char](length)
          System.arraycopy(field.getCharArray, 0, chars, 0, length)
          Denotation(chars, 0, length)
        } else {
          null
        }
      }

      override def deserializeRecyclingOn(record: PactRecord): Denotation = {
        if (index >= 0) {
          val field = record.getField(index, pactField)
          val chars = field.getCharArray
          instance.chars = chars
          instance.start = 0
          instance.length = field.length
          instance
        } else {
          null
        }
      }

      private def readObject(in: java.io.ObjectInputStream) = {
        in.defaultReadObject()
        pactField = new PactString()
        instance = new Denotation(null, 0, 0)
      }
    }
  }

  case class DenotationDataSourceFormat(val charSetName: Option[String] = None) extends DataSourceFormat[Denotation] {

    def this(charSetName: String) = this(Some(charSetName))

    override val stub = classOf[TextInputFormat]

    override def persistConfiguration(config: Configuration) {
      if (charSetName.isDefined)
        config.setString(TextInputFormat.CHARSET_NAME, charSetName.get)

      config.setInteger(TextInputFormat.FIELD_POS, udf.outputFields(0).globalPos.getValue)
    }
  }
}
