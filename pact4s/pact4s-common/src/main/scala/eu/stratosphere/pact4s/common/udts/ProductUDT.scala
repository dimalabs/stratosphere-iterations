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

package eu.stratosphere.pact4s.common.udts

import ProductFactories._

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactInteger

/*
 * Note: These UDT's do NOT handle recursive types, such as:
 * class IntList(_1: Int, _2: IntList) extends Product2[Int, IntList]
 * 
 * A Product2UDT instance for IntList would attempt to flatten
 * the _2 field, which would require an endless recursion in the
 * constructor while initializing the fieldTypes value.
 *  
 * Correct behavior requires boxing recursive types so that
 * the _2 field would be represented by an opaque PactRecord,
 * rather than a flattened list of subfields. Boxing, however,
 * removes the ability to select product fields as keys for
 * Reduce and CoGroup operations.
 * 
 * Because we can't detect whether a generic type T is recursive,
 * we must choose between supporting key selection OR recursion 
 * in these UDT's. We opt to flatten the fields, as key selection 
 * is the more desirable feature and flattening is more efficient
 * than boxing.
 * 
 * Note that key selection is only supported for the default
 * getter names (_1, _2, etc). If a subclass of Product defines
 * other field names, then a corresponding subclass of ProductUDT
 * should also be created to allow selection on those names.
 */

/* REPL script to generate the contents of this file: */

object ProductUDTGenerator {

  def generateProductUDT(arity: Int) = {

    def mkList(template: Int => String) = (1 to arity) map template mkString (", ")
    def mkSepList(sep: String)(template: Int => String) = (1 to arity) map template filter { _ != null } mkString (sep)

    val classDef = "class Product" + arity + "UDT[" + (mkList { "T" + _ + ": UDT" }) + ", R <: Product" + arity + "[" + (mkList { "T" + _ }) + "]: ProductBuilder" + arity + "[" + (mkList { "T" + _ }) + "]#Factory] extends UDT[R] {"
    val fieldTypes = "  override val fieldTypes: Array[Class[_ <: PactValue]] = " + (mkSepList(" ++ ") { "implicitly[UDT[T" + _ + "]].fieldTypes" })
    val createSerializer1 = "  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, " + (mkList { "implicitly[UDT[T" + _ + "]]" }) + ", implicitly[Product" + arity + "Factory[" + (mkList { "T" + _ }) + ", R]])"
    val createSerializer2 = "  private def createSerializer(indexMapTemp: Array[Int], " + (mkList { i => "udtInst" + i + ": UDT[T" + i + "]"}) + ", factory: Product" + arity + "Factory[" + (mkList { "T" + _ }) + ", R]) = new UDTSerializer[R] {"
    val indexVar = "    @transient private val indexMap = indexMapTemp"
    val udtVars = mkSepList("\n") { i => "    @transient private val udt" + i + " = udtInst" + i }
    val innerVars = mkSepList("\n") { i => "    private var inner" + i + ": UDTSerializer[T" + i + "] = null" }
    val indexInits = mkSepList("\n") { i =>
      val in = if (i == 1) "indexMap" else ("rest" + (i - 1))
      val out = if (i == arity - 1) ("indexMap" + (i + 1)) else ("rest" + i)
      if (i < arity)
        "      val (indexMap" + i + ", " + out + ") = " + in + ".splitAt(udt" + i + ".numFields)"
      else
        null
    }
    val innerInits = mkSepList("\n") { i => "      inner" + i + " = udt" + i + ".getSerializer(indexMap" + (if (arity == 1) "" else i) + ")" }
    val init = "    override def init() = {\n" + indexInits + "\n" + innerInits + "\n    }"
    
    val allFieldsIndexes = "      case Seq() => " + mkSepList(" ++ ") { i => "inner" + i + ".getFieldIndex(Seq())" }
    val noFieldsIndexes = """      case _ => throw new NoSuchElementException(selection.mkString("."))"""
    val fieldIndexes = mkSepList("\n") { i => """      case Seq("_""" + i + """", rest @ _*) => inner""" + i + """.getFieldIndex(rest)""" }
    val getFieldIndex = "    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {\n" + allFieldsIndexes + "\n" + fieldIndexes + "\n" + noFieldsIndexes + "\n    }"
      
    val serialize = "    override def serialize(item: R, record: PactRecord) = {" + (mkSepList("") { i => "\n      inner" + i + ".serialize(item._" + i + ", record)" }) + "\n    }"
    val deserialize = "    override def deserialize(record: PactRecord): R = {" + (mkSepList("") { i => "\n      val x" + i + " = inner" + i + ".deserialize(record)" }) +
      "\n      factory.create(" + (mkList { "x" + _ }) + ")\n    }"

    val parts = Seq(classDef, fieldTypes, createSerializer1, createSerializer2, indexVar, udtVars, innerVars, init, getFieldIndex, serialize, deserialize)
    (parts mkString ("\n\n")) + "\n  }\n}"
  }

  def print = println { (1 to 22) map { generateProductUDT(_) } mkString ("\n\n") }
}

// */

class Product1UDT[T1: UDT, R <: Product1[T1]: ProductBuilder1[T1]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[Product1Factory[T1, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], factory: Product1Factory[T1, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1

    private var inner1: UDTSerializer[T1] = null

    override def init() = {

      inner1 = udt1.getSerializer(indexMap)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      factory.create(x1)
    }
  }
}

class Product2UDT[T1: UDT, T2: UDT, R <: Product2[T1, T2]: ProductBuilder2[T1, T2]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[Product2Factory[T1, T2, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], factory: Product2Factory[T1, T2, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null

    override def init() = {
      val (indexMap1, indexMap2) = indexMap.splitAt(udt1.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      factory.create(x1, x2)
    }
  }
}

class Product3UDT[T1: UDT, T2: UDT, T3: UDT, R <: Product3[T1, T2, T3]: ProductBuilder3[T1, T2, T3]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[Product3Factory[T1, T2, T3, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], factory: Product3Factory[T1, T2, T3, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, indexMap3) = rest1.splitAt(udt2.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      factory.create(x1, x2, x3)
    }
  }
}

class Product4UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, R <: Product4[T1, T2, T3, T4]: ProductBuilder4[T1, T2, T3, T4]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[Product4Factory[T1, T2, T3, T4, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], factory: Product4Factory[T1, T2, T3, T4, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, indexMap4) = rest2.splitAt(udt3.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      factory.create(x1, x2, x3, x4)
    }
  }
}

class Product5UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, R <: Product5[T1, T2, T3, T4, T5]: ProductBuilder5[T1, T2, T3, T4, T5]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[Product5Factory[T1, T2, T3, T4, T5, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], factory: Product5Factory[T1, T2, T3, T4, T5, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, indexMap5) = rest3.splitAt(udt4.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      factory.create(x1, x2, x3, x4, x5)
    }
  }
}

class Product6UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, R <: Product6[T1, T2, T3, T4, T5, T6]: ProductBuilder6[T1, T2, T3, T4, T5, T6]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[Product6Factory[T1, T2, T3, T4, T5, T6, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], factory: Product6Factory[T1, T2, T3, T4, T5, T6, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, indexMap6) = rest4.splitAt(udt5.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6)
    }
  }
}

class Product7UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, R <: Product7[T1, T2, T3, T4, T5, T6, T7]: ProductBuilder7[T1, T2, T3, T4, T5, T6, T7]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[Product7Factory[T1, T2, T3, T4, T5, T6, T7, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], factory: Product7Factory[T1, T2, T3, T4, T5, T6, T7, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, indexMap7) = rest5.splitAt(udt6.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7)
    }
  }
}

class Product8UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, R <: Product8[T1, T2, T3, T4, T5, T6, T7, T8]: ProductBuilder8[T1, T2, T3, T4, T5, T6, T7, T8]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[Product8Factory[T1, T2, T3, T4, T5, T6, T7, T8, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], factory: Product8Factory[T1, T2, T3, T4, T5, T6, T7, T8, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, indexMap8) = rest6.splitAt(udt7.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8)
    }
  }
}

class Product9UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, R <: Product9[T1, T2, T3, T4, T5, T6, T7, T8, T9]: ProductBuilder9[T1, T2, T3, T4, T5, T6, T7, T8, T9]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[Product9Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], factory: Product9Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, indexMap9) = rest7.splitAt(udt8.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9)
    }
  }
}

class Product10UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, R <: Product10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]: ProductBuilder10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[Product10Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], factory: Product10Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, indexMap10) = rest8.splitAt(udt9.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10)
    }
  }
}

class Product11UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, R <: Product11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]: ProductBuilder11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[Product11Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], factory: Product11Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, indexMap11) = rest9.splitAt(udt10.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11)
    }
  }
}

class Product12UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, R <: Product12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]: ProductBuilder12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[Product12Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], factory: Product12Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, indexMap12) = rest10.splitAt(udt11.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12)
    }
  }
}

class Product13UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, R <: Product13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]: ProductBuilder13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[Product13Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], factory: Product13Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, indexMap13) = rest11.splitAt(udt12.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
    }
  }
}

class Product14UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, R <: Product14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]: ProductBuilder14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[Product14Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], factory: Product14Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, indexMap14) = rest12.splitAt(udt13.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14)
    }
  }
}

class Product15UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, R <: Product15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]: ProductBuilder15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[Product15Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], factory: Product15Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, indexMap15) = rest13.splitAt(udt14.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15)
    }
  }
}

class Product16UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, R <: Product16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]: ProductBuilder16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[UDT[T16]], implicitly[Product16Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], udtInst16: UDT[T16], factory: Product16Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15
    @transient private val udt16 = udtInst16

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null
    private var inner16: UDTSerializer[T16] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, rest14) = rest13.splitAt(udt14.numFields)
      val (indexMap15, indexMap16) = rest14.splitAt(udt15.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
      inner16 = udt16.getSerializer(indexMap16)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq()) ++ inner16.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case Seq("_16", rest @ _*) => inner16.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
      inner16.serialize(item._16, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      val x16 = inner16.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16)
    }
  }
}

class Product17UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, R <: Product17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]: ProductBuilder17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[UDT[T16]], implicitly[UDT[T17]], implicitly[Product17Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], udtInst16: UDT[T16], udtInst17: UDT[T17], factory: Product17Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15
    @transient private val udt16 = udtInst16
    @transient private val udt17 = udtInst17

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null
    private var inner16: UDTSerializer[T16] = null
    private var inner17: UDTSerializer[T17] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, rest14) = rest13.splitAt(udt14.numFields)
      val (indexMap15, rest15) = rest14.splitAt(udt15.numFields)
      val (indexMap16, indexMap17) = rest15.splitAt(udt16.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
      inner16 = udt16.getSerializer(indexMap16)
      inner17 = udt17.getSerializer(indexMap17)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq()) ++ inner16.getFieldIndex(Seq()) ++ inner17.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case Seq("_16", rest @ _*) => inner16.getFieldIndex(rest)
      case Seq("_17", rest @ _*) => inner17.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
      inner16.serialize(item._16, record)
      inner17.serialize(item._17, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      val x16 = inner16.deserialize(record)
      val x17 = inner17.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17)
    }
  }
}

class Product18UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, R <: Product18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]: ProductBuilder18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[UDT[T16]], implicitly[UDT[T17]], implicitly[UDT[T18]], implicitly[Product18Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], udtInst16: UDT[T16], udtInst17: UDT[T17], udtInst18: UDT[T18], factory: Product18Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15
    @transient private val udt16 = udtInst16
    @transient private val udt17 = udtInst17
    @transient private val udt18 = udtInst18

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null
    private var inner16: UDTSerializer[T16] = null
    private var inner17: UDTSerializer[T17] = null
    private var inner18: UDTSerializer[T18] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, rest14) = rest13.splitAt(udt14.numFields)
      val (indexMap15, rest15) = rest14.splitAt(udt15.numFields)
      val (indexMap16, rest16) = rest15.splitAt(udt16.numFields)
      val (indexMap17, indexMap18) = rest16.splitAt(udt17.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
      inner16 = udt16.getSerializer(indexMap16)
      inner17 = udt17.getSerializer(indexMap17)
      inner18 = udt18.getSerializer(indexMap18)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq()) ++ inner16.getFieldIndex(Seq()) ++ inner17.getFieldIndex(Seq()) ++ inner18.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case Seq("_16", rest @ _*) => inner16.getFieldIndex(rest)
      case Seq("_17", rest @ _*) => inner17.getFieldIndex(rest)
      case Seq("_18", rest @ _*) => inner18.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
      inner16.serialize(item._16, record)
      inner17.serialize(item._17, record)
      inner18.serialize(item._18, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      val x16 = inner16.deserialize(record)
      val x17 = inner17.deserialize(record)
      val x18 = inner18.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18)
    }
  }
}

class Product19UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, R <: Product19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]: ProductBuilder19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[UDT[T16]], implicitly[UDT[T17]], implicitly[UDT[T18]], implicitly[UDT[T19]], implicitly[Product19Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], udtInst16: UDT[T16], udtInst17: UDT[T17], udtInst18: UDT[T18], udtInst19: UDT[T19], factory: Product19Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15
    @transient private val udt16 = udtInst16
    @transient private val udt17 = udtInst17
    @transient private val udt18 = udtInst18
    @transient private val udt19 = udtInst19

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null
    private var inner16: UDTSerializer[T16] = null
    private var inner17: UDTSerializer[T17] = null
    private var inner18: UDTSerializer[T18] = null
    private var inner19: UDTSerializer[T19] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, rest14) = rest13.splitAt(udt14.numFields)
      val (indexMap15, rest15) = rest14.splitAt(udt15.numFields)
      val (indexMap16, rest16) = rest15.splitAt(udt16.numFields)
      val (indexMap17, rest17) = rest16.splitAt(udt17.numFields)
      val (indexMap18, indexMap19) = rest17.splitAt(udt18.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
      inner16 = udt16.getSerializer(indexMap16)
      inner17 = udt17.getSerializer(indexMap17)
      inner18 = udt18.getSerializer(indexMap18)
      inner19 = udt19.getSerializer(indexMap19)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq()) ++ inner16.getFieldIndex(Seq()) ++ inner17.getFieldIndex(Seq()) ++ inner18.getFieldIndex(Seq()) ++ inner19.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case Seq("_16", rest @ _*) => inner16.getFieldIndex(rest)
      case Seq("_17", rest @ _*) => inner17.getFieldIndex(rest)
      case Seq("_18", rest @ _*) => inner18.getFieldIndex(rest)
      case Seq("_19", rest @ _*) => inner19.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
      inner16.serialize(item._16, record)
      inner17.serialize(item._17, record)
      inner18.serialize(item._18, record)
      inner19.serialize(item._19, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      val x16 = inner16.deserialize(record)
      val x17 = inner17.deserialize(record)
      val x18 = inner18.deserialize(record)
      val x19 = inner19.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19)
    }
  }
}

class Product20UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, R <: Product20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]: ProductBuilder20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes ++ implicitly[UDT[T20]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[UDT[T16]], implicitly[UDT[T17]], implicitly[UDT[T18]], implicitly[UDT[T19]], implicitly[UDT[T20]], implicitly[Product20Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], udtInst16: UDT[T16], udtInst17: UDT[T17], udtInst18: UDT[T18], udtInst19: UDT[T19], udtInst20: UDT[T20], factory: Product20Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15
    @transient private val udt16 = udtInst16
    @transient private val udt17 = udtInst17
    @transient private val udt18 = udtInst18
    @transient private val udt19 = udtInst19
    @transient private val udt20 = udtInst20

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null
    private var inner16: UDTSerializer[T16] = null
    private var inner17: UDTSerializer[T17] = null
    private var inner18: UDTSerializer[T18] = null
    private var inner19: UDTSerializer[T19] = null
    private var inner20: UDTSerializer[T20] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, rest14) = rest13.splitAt(udt14.numFields)
      val (indexMap15, rest15) = rest14.splitAt(udt15.numFields)
      val (indexMap16, rest16) = rest15.splitAt(udt16.numFields)
      val (indexMap17, rest17) = rest16.splitAt(udt17.numFields)
      val (indexMap18, rest18) = rest17.splitAt(udt18.numFields)
      val (indexMap19, indexMap20) = rest18.splitAt(udt19.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
      inner16 = udt16.getSerializer(indexMap16)
      inner17 = udt17.getSerializer(indexMap17)
      inner18 = udt18.getSerializer(indexMap18)
      inner19 = udt19.getSerializer(indexMap19)
      inner20 = udt20.getSerializer(indexMap20)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq()) ++ inner16.getFieldIndex(Seq()) ++ inner17.getFieldIndex(Seq()) ++ inner18.getFieldIndex(Seq()) ++ inner19.getFieldIndex(Seq()) ++ inner20.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case Seq("_16", rest @ _*) => inner16.getFieldIndex(rest)
      case Seq("_17", rest @ _*) => inner17.getFieldIndex(rest)
      case Seq("_18", rest @ _*) => inner18.getFieldIndex(rest)
      case Seq("_19", rest @ _*) => inner19.getFieldIndex(rest)
      case Seq("_20", rest @ _*) => inner20.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
      inner16.serialize(item._16, record)
      inner17.serialize(item._17, record)
      inner18.serialize(item._18, record)
      inner19.serialize(item._19, record)
      inner20.serialize(item._20, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      val x16 = inner16.deserialize(record)
      val x17 = inner17.deserialize(record)
      val x18 = inner18.deserialize(record)
      val x19 = inner19.deserialize(record)
      val x20 = inner20.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20)
    }
  }
}

class Product21UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, T21: UDT, R <: Product21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]: ProductBuilder21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes ++ implicitly[UDT[T20]].fieldTypes ++ implicitly[UDT[T21]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[UDT[T16]], implicitly[UDT[T17]], implicitly[UDT[T18]], implicitly[UDT[T19]], implicitly[UDT[T20]], implicitly[UDT[T21]], implicitly[Product21Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], udtInst16: UDT[T16], udtInst17: UDT[T17], udtInst18: UDT[T18], udtInst19: UDT[T19], udtInst20: UDT[T20], udtInst21: UDT[T21], factory: Product21Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15
    @transient private val udt16 = udtInst16
    @transient private val udt17 = udtInst17
    @transient private val udt18 = udtInst18
    @transient private val udt19 = udtInst19
    @transient private val udt20 = udtInst20
    @transient private val udt21 = udtInst21

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null
    private var inner16: UDTSerializer[T16] = null
    private var inner17: UDTSerializer[T17] = null
    private var inner18: UDTSerializer[T18] = null
    private var inner19: UDTSerializer[T19] = null
    private var inner20: UDTSerializer[T20] = null
    private var inner21: UDTSerializer[T21] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, rest14) = rest13.splitAt(udt14.numFields)
      val (indexMap15, rest15) = rest14.splitAt(udt15.numFields)
      val (indexMap16, rest16) = rest15.splitAt(udt16.numFields)
      val (indexMap17, rest17) = rest16.splitAt(udt17.numFields)
      val (indexMap18, rest18) = rest17.splitAt(udt18.numFields)
      val (indexMap19, rest19) = rest18.splitAt(udt19.numFields)
      val (indexMap20, indexMap21) = rest19.splitAt(udt20.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
      inner16 = udt16.getSerializer(indexMap16)
      inner17 = udt17.getSerializer(indexMap17)
      inner18 = udt18.getSerializer(indexMap18)
      inner19 = udt19.getSerializer(indexMap19)
      inner20 = udt20.getSerializer(indexMap20)
      inner21 = udt21.getSerializer(indexMap21)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq()) ++ inner16.getFieldIndex(Seq()) ++ inner17.getFieldIndex(Seq()) ++ inner18.getFieldIndex(Seq()) ++ inner19.getFieldIndex(Seq()) ++ inner20.getFieldIndex(Seq()) ++ inner21.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case Seq("_16", rest @ _*) => inner16.getFieldIndex(rest)
      case Seq("_17", rest @ _*) => inner17.getFieldIndex(rest)
      case Seq("_18", rest @ _*) => inner18.getFieldIndex(rest)
      case Seq("_19", rest @ _*) => inner19.getFieldIndex(rest)
      case Seq("_20", rest @ _*) => inner20.getFieldIndex(rest)
      case Seq("_21", rest @ _*) => inner21.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
      inner16.serialize(item._16, record)
      inner17.serialize(item._17, record)
      inner18.serialize(item._18, record)
      inner19.serialize(item._19, record)
      inner20.serialize(item._20, record)
      inner21.serialize(item._21, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      val x16 = inner16.deserialize(record)
      val x17 = inner17.deserialize(record)
      val x18 = inner18.deserialize(record)
      val x19 = inner19.deserialize(record)
      val x20 = inner20.deserialize(record)
      val x21 = inner21.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21)
    }
  }
}

class Product22UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, T21: UDT, T22: UDT, R <: Product22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]: ProductBuilder22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes ++ implicitly[UDT[T20]].fieldTypes ++ implicitly[UDT[T21]].fieldTypes ++ implicitly[UDT[T22]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = createSerializer(indexMap, implicitly[UDT[T1]], implicitly[UDT[T2]], implicitly[UDT[T3]], implicitly[UDT[T4]], implicitly[UDT[T5]], implicitly[UDT[T6]], implicitly[UDT[T7]], implicitly[UDT[T8]], implicitly[UDT[T9]], implicitly[UDT[T10]], implicitly[UDT[T11]], implicitly[UDT[T12]], implicitly[UDT[T13]], implicitly[UDT[T14]], implicitly[UDT[T15]], implicitly[UDT[T16]], implicitly[UDT[T17]], implicitly[UDT[T18]], implicitly[UDT[T19]], implicitly[UDT[T20]], implicitly[UDT[T21]], implicitly[UDT[T22]], implicitly[Product22Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, R]])

  private def createSerializer(indexMapTemp: Array[Int], udtInst1: UDT[T1], udtInst2: UDT[T2], udtInst3: UDT[T3], udtInst4: UDT[T4], udtInst5: UDT[T5], udtInst6: UDT[T6], udtInst7: UDT[T7], udtInst8: UDT[T8], udtInst9: UDT[T9], udtInst10: UDT[T10], udtInst11: UDT[T11], udtInst12: UDT[T12], udtInst13: UDT[T13], udtInst14: UDT[T14], udtInst15: UDT[T15], udtInst16: UDT[T16], udtInst17: UDT[T17], udtInst18: UDT[T18], udtInst19: UDT[T19], udtInst20: UDT[T20], udtInst21: UDT[T21], udtInst22: UDT[T22], factory: Product22Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, R]) = new UDTSerializer[R] {

    @transient private val indexMap = indexMapTemp

    @transient private val udt1 = udtInst1
    @transient private val udt2 = udtInst2
    @transient private val udt3 = udtInst3
    @transient private val udt4 = udtInst4
    @transient private val udt5 = udtInst5
    @transient private val udt6 = udtInst6
    @transient private val udt7 = udtInst7
    @transient private val udt8 = udtInst8
    @transient private val udt9 = udtInst9
    @transient private val udt10 = udtInst10
    @transient private val udt11 = udtInst11
    @transient private val udt12 = udtInst12
    @transient private val udt13 = udtInst13
    @transient private val udt14 = udtInst14
    @transient private val udt15 = udtInst15
    @transient private val udt16 = udtInst16
    @transient private val udt17 = udtInst17
    @transient private val udt18 = udtInst18
    @transient private val udt19 = udtInst19
    @transient private val udt20 = udtInst20
    @transient private val udt21 = udtInst21
    @transient private val udt22 = udtInst22

    private var inner1: UDTSerializer[T1] = null
    private var inner2: UDTSerializer[T2] = null
    private var inner3: UDTSerializer[T3] = null
    private var inner4: UDTSerializer[T4] = null
    private var inner5: UDTSerializer[T5] = null
    private var inner6: UDTSerializer[T6] = null
    private var inner7: UDTSerializer[T7] = null
    private var inner8: UDTSerializer[T8] = null
    private var inner9: UDTSerializer[T9] = null
    private var inner10: UDTSerializer[T10] = null
    private var inner11: UDTSerializer[T11] = null
    private var inner12: UDTSerializer[T12] = null
    private var inner13: UDTSerializer[T13] = null
    private var inner14: UDTSerializer[T14] = null
    private var inner15: UDTSerializer[T15] = null
    private var inner16: UDTSerializer[T16] = null
    private var inner17: UDTSerializer[T17] = null
    private var inner18: UDTSerializer[T18] = null
    private var inner19: UDTSerializer[T19] = null
    private var inner20: UDTSerializer[T20] = null
    private var inner21: UDTSerializer[T21] = null
    private var inner22: UDTSerializer[T22] = null

    override def init() = {
      val (indexMap1, rest1) = indexMap.splitAt(udt1.numFields)
      val (indexMap2, rest2) = rest1.splitAt(udt2.numFields)
      val (indexMap3, rest3) = rest2.splitAt(udt3.numFields)
      val (indexMap4, rest4) = rest3.splitAt(udt4.numFields)
      val (indexMap5, rest5) = rest4.splitAt(udt5.numFields)
      val (indexMap6, rest6) = rest5.splitAt(udt6.numFields)
      val (indexMap7, rest7) = rest6.splitAt(udt7.numFields)
      val (indexMap8, rest8) = rest7.splitAt(udt8.numFields)
      val (indexMap9, rest9) = rest8.splitAt(udt9.numFields)
      val (indexMap10, rest10) = rest9.splitAt(udt10.numFields)
      val (indexMap11, rest11) = rest10.splitAt(udt11.numFields)
      val (indexMap12, rest12) = rest11.splitAt(udt12.numFields)
      val (indexMap13, rest13) = rest12.splitAt(udt13.numFields)
      val (indexMap14, rest14) = rest13.splitAt(udt14.numFields)
      val (indexMap15, rest15) = rest14.splitAt(udt15.numFields)
      val (indexMap16, rest16) = rest15.splitAt(udt16.numFields)
      val (indexMap17, rest17) = rest16.splitAt(udt17.numFields)
      val (indexMap18, rest18) = rest17.splitAt(udt18.numFields)
      val (indexMap19, rest19) = rest18.splitAt(udt19.numFields)
      val (indexMap20, rest20) = rest19.splitAt(udt20.numFields)
      val (indexMap21, indexMap22) = rest20.splitAt(udt21.numFields)
      inner1 = udt1.getSerializer(indexMap1)
      inner2 = udt2.getSerializer(indexMap2)
      inner3 = udt3.getSerializer(indexMap3)
      inner4 = udt4.getSerializer(indexMap4)
      inner5 = udt5.getSerializer(indexMap5)
      inner6 = udt6.getSerializer(indexMap6)
      inner7 = udt7.getSerializer(indexMap7)
      inner8 = udt8.getSerializer(indexMap8)
      inner9 = udt9.getSerializer(indexMap9)
      inner10 = udt10.getSerializer(indexMap10)
      inner11 = udt11.getSerializer(indexMap11)
      inner12 = udt12.getSerializer(indexMap12)
      inner13 = udt13.getSerializer(indexMap13)
      inner14 = udt14.getSerializer(indexMap14)
      inner15 = udt15.getSerializer(indexMap15)
      inner16 = udt16.getSerializer(indexMap16)
      inner17 = udt17.getSerializer(indexMap17)
      inner18 = udt18.getSerializer(indexMap18)
      inner19 = udt19.getSerializer(indexMap19)
      inner20 = udt20.getSerializer(indexMap20)
      inner21 = udt21.getSerializer(indexMap21)
      inner22 = udt22.getSerializer(indexMap22)
    }

    override def getFieldIndex(selection: Seq[String]): List[Int] = selection match {
      case Seq() => inner1.getFieldIndex(Seq()) ++ inner2.getFieldIndex(Seq()) ++ inner3.getFieldIndex(Seq()) ++ inner4.getFieldIndex(Seq()) ++ inner5.getFieldIndex(Seq()) ++ inner6.getFieldIndex(Seq()) ++ inner7.getFieldIndex(Seq()) ++ inner8.getFieldIndex(Seq()) ++ inner9.getFieldIndex(Seq()) ++ inner10.getFieldIndex(Seq()) ++ inner11.getFieldIndex(Seq()) ++ inner12.getFieldIndex(Seq()) ++ inner13.getFieldIndex(Seq()) ++ inner14.getFieldIndex(Seq()) ++ inner15.getFieldIndex(Seq()) ++ inner16.getFieldIndex(Seq()) ++ inner17.getFieldIndex(Seq()) ++ inner18.getFieldIndex(Seq()) ++ inner19.getFieldIndex(Seq()) ++ inner20.getFieldIndex(Seq()) ++ inner21.getFieldIndex(Seq()) ++ inner22.getFieldIndex(Seq())
      case Seq("_1", rest @ _*) => inner1.getFieldIndex(rest)
      case Seq("_2", rest @ _*) => inner2.getFieldIndex(rest)
      case Seq("_3", rest @ _*) => inner3.getFieldIndex(rest)
      case Seq("_4", rest @ _*) => inner4.getFieldIndex(rest)
      case Seq("_5", rest @ _*) => inner5.getFieldIndex(rest)
      case Seq("_6", rest @ _*) => inner6.getFieldIndex(rest)
      case Seq("_7", rest @ _*) => inner7.getFieldIndex(rest)
      case Seq("_8", rest @ _*) => inner8.getFieldIndex(rest)
      case Seq("_9", rest @ _*) => inner9.getFieldIndex(rest)
      case Seq("_10", rest @ _*) => inner10.getFieldIndex(rest)
      case Seq("_11", rest @ _*) => inner11.getFieldIndex(rest)
      case Seq("_12", rest @ _*) => inner12.getFieldIndex(rest)
      case Seq("_13", rest @ _*) => inner13.getFieldIndex(rest)
      case Seq("_14", rest @ _*) => inner14.getFieldIndex(rest)
      case Seq("_15", rest @ _*) => inner15.getFieldIndex(rest)
      case Seq("_16", rest @ _*) => inner16.getFieldIndex(rest)
      case Seq("_17", rest @ _*) => inner17.getFieldIndex(rest)
      case Seq("_18", rest @ _*) => inner18.getFieldIndex(rest)
      case Seq("_19", rest @ _*) => inner19.getFieldIndex(rest)
      case Seq("_20", rest @ _*) => inner20.getFieldIndex(rest)
      case Seq("_21", rest @ _*) => inner21.getFieldIndex(rest)
      case Seq("_22", rest @ _*) => inner22.getFieldIndex(rest)
      case _ => throw new NoSuchElementException(selection.mkString("."))
    }

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
      inner4.serialize(item._4, record)
      inner5.serialize(item._5, record)
      inner6.serialize(item._6, record)
      inner7.serialize(item._7, record)
      inner8.serialize(item._8, record)
      inner9.serialize(item._9, record)
      inner10.serialize(item._10, record)
      inner11.serialize(item._11, record)
      inner12.serialize(item._12, record)
      inner13.serialize(item._13, record)
      inner14.serialize(item._14, record)
      inner15.serialize(item._15, record)
      inner16.serialize(item._16, record)
      inner17.serialize(item._17, record)
      inner18.serialize(item._18, record)
      inner19.serialize(item._19, record)
      inner20.serialize(item._20, record)
      inner21.serialize(item._21, record)
      inner22.serialize(item._22, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      val x4 = inner4.deserialize(record)
      val x5 = inner5.deserialize(record)
      val x6 = inner6.deserialize(record)
      val x7 = inner7.deserialize(record)
      val x8 = inner8.deserialize(record)
      val x9 = inner9.deserialize(record)
      val x10 = inner10.deserialize(record)
      val x11 = inner11.deserialize(record)
      val x12 = inner12.deserialize(record)
      val x13 = inner13.deserialize(record)
      val x14 = inner14.deserialize(record)
      val x15 = inner15.deserialize(record)
      val x16 = inner16.deserialize(record)
      val x17 = inner17.deserialize(record)
      val x18 = inner18.deserialize(record)
      val x19 = inner19.deserialize(record)
      val x20 = inner20.deserialize(record)
      val x21 = inner21.deserialize(record)
      val x22 = inner22.deserialize(record)
      factory.create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21, x22)
    }
  }
}

