package eu.stratosphere.pact4s.common.udts

import ProductFactories._

import eu.stratosphere.pact4s.common.analyzer._

import java.io.ObjectInputStream
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.base.PactInteger

/* REPL script to generate the contents of this file: 
object ProductUDTGenerator {

  def generateProductUDT(arity: Int) = {

    def mkList(template: Int => String) = (1 to arity) map template mkString (", ")
    def mkSepList(sep: String)(template: Int => String) = (1 to arity) map template filter { _ != null } mkString (sep)

    val classDef = "class Product" + arity + "UDT[" + (mkList { "T" + _ + ": UDT" }) + ", R <: Product" + arity + "[" + (mkList { "T" + _ }) + "]: ProductBuilder" + arity + "[" + (mkList { "T" + _ }) + "]#Factory] extends UDT[R] {"
    val fieldTypes = "  override val fieldTypes: Array[Class[_ <: PactValue]] = " + (mkSepList(" ++ ") { "implicitly[UDT[T" + _ + "]].fieldTypes" })
    val createSerializer = "  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {"
    val indexMaps = mkSepList("\n") { i =>
      val in = if (i == 1) "indexMap" else ("rest" + (i - 1))
      val out = if (i == arity - 1) ("indexMap" + (i + 1)) else ("rest" + i)
      if (i < arity)
        "    private val (indexMap" + i + ", " + out + ") = " + in + ".splitAt(implicitly[UDT[T" + i + "]].numFields)"
      else
        null
    }
    val inners = mkSepList("\n") { i => "    private val inner" + i + " = implicitly[UDT[T" + i + "]].createSerializer(indexMap" + (if (arity == 1) "" else i) + ")" }
    val serialize = "    override def serialize(item: R, record: PactRecord) = {" + (mkSepList("") { i => "\n      inner" + i + ".serialize(item._" + i + ", record)" }) + "\n    }"
    val deserialize = "    override def deserialize(record: PactRecord): R = {" + (mkSepList("") { i => "\n      val x" + i + " = inner" + i + ".deserialize(record)" }) +
      "\n      implicitly[Product" + arity + "Factory[" + (mkList { "T" + _ }) + ", R]].create(" + (mkList { "x" + _ }) + ")\n    }"

    val parts = Seq(classDef, fieldTypes, createSerializer, indexMaps, inners, serialize, deserialize)
    (parts mkString ("\n\n")) + "\n  }\n}"
  }

  println { (1 to 22) map { generateProductUDT(_) } mkString ("\n\n") }
}
// */

class Product1UDT[T1: UDT, R <: Product1[T1]: ProductBuilder1[T1]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap)

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      implicitly[Product1Factory[T1, R]].create(x1)
    }
  }
}

class Product2UDT[T1: UDT, T2: UDT, R <: Product2[T1, T2]: ProductBuilder2[T1, T2]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, indexMap2) = indexMap.splitAt(implicitly[UDT[T1]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      implicitly[Product2Factory[T1, T2, R]].create(x1, x2)
    }
  }
}

class Product3UDT[T1: UDT, T2: UDT, T3: UDT, R <: Product3[T1, T2, T3]: ProductBuilder3[T1, T2, T3]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, indexMap3) = rest1.splitAt(implicitly[UDT[T2]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)

    override def serialize(item: R, record: PactRecord) = {
      inner1.serialize(item._1, record)
      inner2.serialize(item._2, record)
      inner3.serialize(item._3, record)
    }

    override def deserialize(record: PactRecord): R = {
      val x1 = inner1.deserialize(record)
      val x2 = inner2.deserialize(record)
      val x3 = inner3.deserialize(record)
      implicitly[Product3Factory[T1, T2, T3, R]].create(x1, x2, x3)
    }
  }
}

class Product4UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, R <: Product4[T1, T2, T3, T4]: ProductBuilder4[T1, T2, T3, T4]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, indexMap4) = rest2.splitAt(implicitly[UDT[T3]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)

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
      implicitly[Product4Factory[T1, T2, T3, T4, R]].create(x1, x2, x3, x4)
    }
  }
}

class Product5UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, R <: Product5[T1, T2, T3, T4, T5]: ProductBuilder5[T1, T2, T3, T4, T5]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, indexMap5) = rest3.splitAt(implicitly[UDT[T4]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)

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
      implicitly[Product5Factory[T1, T2, T3, T4, T5, R]].create(x1, x2, x3, x4, x5)
    }
  }
}

class Product6UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, R <: Product6[T1, T2, T3, T4, T5, T6]: ProductBuilder6[T1, T2, T3, T4, T5, T6]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, indexMap6) = rest4.splitAt(implicitly[UDT[T5]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)

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
      implicitly[Product6Factory[T1, T2, T3, T4, T5, T6, R]].create(x1, x2, x3, x4, x5, x6)
    }
  }
}

class Product7UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, R <: Product7[T1, T2, T3, T4, T5, T6, T7]: ProductBuilder7[T1, T2, T3, T4, T5, T6, T7]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, indexMap7) = rest5.splitAt(implicitly[UDT[T6]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)

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
      implicitly[Product7Factory[T1, T2, T3, T4, T5, T6, T7, R]].create(x1, x2, x3, x4, x5, x6, x7)
    }
  }
}

class Product8UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, R <: Product8[T1, T2, T3, T4, T5, T6, T7, T8]: ProductBuilder8[T1, T2, T3, T4, T5, T6, T7, T8]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, indexMap8) = rest6.splitAt(implicitly[UDT[T7]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)

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
      implicitly[Product8Factory[T1, T2, T3, T4, T5, T6, T7, T8, R]].create(x1, x2, x3, x4, x5, x6, x7, x8)
    }
  }
}

class Product9UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, R <: Product9[T1, T2, T3, T4, T5, T6, T7, T8, T9]: ProductBuilder9[T1, T2, T3, T4, T5, T6, T7, T8, T9]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, indexMap9) = rest7.splitAt(implicitly[UDT[T8]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)

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
      implicitly[Product9Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9)
    }
  }
}

class Product10UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, R <: Product10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]: ProductBuilder10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, indexMap10) = rest8.splitAt(implicitly[UDT[T9]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)

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
      implicitly[Product10Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10)
    }
  }
}

class Product11UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, R <: Product11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]: ProductBuilder11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, indexMap11) = rest9.splitAt(implicitly[UDT[T10]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)

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
      implicitly[Product11Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11)
    }
  }
}

class Product12UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, R <: Product12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]: ProductBuilder12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, indexMap12) = rest10.splitAt(implicitly[UDT[T11]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)

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
      implicitly[Product12Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12)
    }
  }
}

class Product13UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, R <: Product13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]: ProductBuilder13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, indexMap13) = rest11.splitAt(implicitly[UDT[T12]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)

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
      implicitly[Product13Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13)
    }
  }
}

class Product14UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, R <: Product14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]: ProductBuilder14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, indexMap14) = rest12.splitAt(implicitly[UDT[T13]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)

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
      implicitly[Product14Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14)
    }
  }
}

class Product15UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, R <: Product15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]: ProductBuilder15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, indexMap15) = rest13.splitAt(implicitly[UDT[T14]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)

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
      implicitly[Product15Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15)
    }
  }
}

class Product16UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, R <: Product16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]: ProductBuilder16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, rest14) = rest13.splitAt(implicitly[UDT[T14]].numFields)
    private val (indexMap15, indexMap16) = rest14.splitAt(implicitly[UDT[T15]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)
    private val inner16 = implicitly[UDT[T16]].createSerializer(indexMap16)

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
      implicitly[Product16Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16)
    }
  }
}

class Product17UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, R <: Product17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]: ProductBuilder17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, rest14) = rest13.splitAt(implicitly[UDT[T14]].numFields)
    private val (indexMap15, rest15) = rest14.splitAt(implicitly[UDT[T15]].numFields)
    private val (indexMap16, indexMap17) = rest15.splitAt(implicitly[UDT[T16]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)
    private val inner16 = implicitly[UDT[T16]].createSerializer(indexMap16)
    private val inner17 = implicitly[UDT[T17]].createSerializer(indexMap17)

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
      implicitly[Product17Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17)
    }
  }
}

class Product18UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, R <: Product18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]: ProductBuilder18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, rest14) = rest13.splitAt(implicitly[UDT[T14]].numFields)
    private val (indexMap15, rest15) = rest14.splitAt(implicitly[UDT[T15]].numFields)
    private val (indexMap16, rest16) = rest15.splitAt(implicitly[UDT[T16]].numFields)
    private val (indexMap17, indexMap18) = rest16.splitAt(implicitly[UDT[T17]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)
    private val inner16 = implicitly[UDT[T16]].createSerializer(indexMap16)
    private val inner17 = implicitly[UDT[T17]].createSerializer(indexMap17)
    private val inner18 = implicitly[UDT[T18]].createSerializer(indexMap18)

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
      implicitly[Product18Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18)
    }
  }
}

class Product19UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, R <: Product19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]: ProductBuilder19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, rest14) = rest13.splitAt(implicitly[UDT[T14]].numFields)
    private val (indexMap15, rest15) = rest14.splitAt(implicitly[UDT[T15]].numFields)
    private val (indexMap16, rest16) = rest15.splitAt(implicitly[UDT[T16]].numFields)
    private val (indexMap17, rest17) = rest16.splitAt(implicitly[UDT[T17]].numFields)
    private val (indexMap18, indexMap19) = rest17.splitAt(implicitly[UDT[T18]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)
    private val inner16 = implicitly[UDT[T16]].createSerializer(indexMap16)
    private val inner17 = implicitly[UDT[T17]].createSerializer(indexMap17)
    private val inner18 = implicitly[UDT[T18]].createSerializer(indexMap18)
    private val inner19 = implicitly[UDT[T19]].createSerializer(indexMap19)

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
      implicitly[Product19Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19)
    }
  }
}

class Product20UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, R <: Product20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]: ProductBuilder20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes ++ implicitly[UDT[T20]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, rest14) = rest13.splitAt(implicitly[UDT[T14]].numFields)
    private val (indexMap15, rest15) = rest14.splitAt(implicitly[UDT[T15]].numFields)
    private val (indexMap16, rest16) = rest15.splitAt(implicitly[UDT[T16]].numFields)
    private val (indexMap17, rest17) = rest16.splitAt(implicitly[UDT[T17]].numFields)
    private val (indexMap18, rest18) = rest17.splitAt(implicitly[UDT[T18]].numFields)
    private val (indexMap19, indexMap20) = rest18.splitAt(implicitly[UDT[T19]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)
    private val inner16 = implicitly[UDT[T16]].createSerializer(indexMap16)
    private val inner17 = implicitly[UDT[T17]].createSerializer(indexMap17)
    private val inner18 = implicitly[UDT[T18]].createSerializer(indexMap18)
    private val inner19 = implicitly[UDT[T19]].createSerializer(indexMap19)
    private val inner20 = implicitly[UDT[T20]].createSerializer(indexMap20)

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
      implicitly[Product20Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20)
    }
  }
}

class Product21UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, T21: UDT, R <: Product21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]: ProductBuilder21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes ++ implicitly[UDT[T20]].fieldTypes ++ implicitly[UDT[T21]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, rest14) = rest13.splitAt(implicitly[UDT[T14]].numFields)
    private val (indexMap15, rest15) = rest14.splitAt(implicitly[UDT[T15]].numFields)
    private val (indexMap16, rest16) = rest15.splitAt(implicitly[UDT[T16]].numFields)
    private val (indexMap17, rest17) = rest16.splitAt(implicitly[UDT[T17]].numFields)
    private val (indexMap18, rest18) = rest17.splitAt(implicitly[UDT[T18]].numFields)
    private val (indexMap19, rest19) = rest18.splitAt(implicitly[UDT[T19]].numFields)
    private val (indexMap20, indexMap21) = rest19.splitAt(implicitly[UDT[T20]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)
    private val inner16 = implicitly[UDT[T16]].createSerializer(indexMap16)
    private val inner17 = implicitly[UDT[T17]].createSerializer(indexMap17)
    private val inner18 = implicitly[UDT[T18]].createSerializer(indexMap18)
    private val inner19 = implicitly[UDT[T19]].createSerializer(indexMap19)
    private val inner20 = implicitly[UDT[T20]].createSerializer(indexMap20)
    private val inner21 = implicitly[UDT[T21]].createSerializer(indexMap21)

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
      implicitly[Product21Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21)
    }
  }
}

class Product22UDT[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, T21: UDT, T22: UDT, R <: Product22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]: ProductBuilder22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]#Factory] extends UDT[R] {

  override val fieldTypes: Array[Class[_ <: PactValue]] = implicitly[UDT[T1]].fieldTypes ++ implicitly[UDT[T2]].fieldTypes ++ implicitly[UDT[T3]].fieldTypes ++ implicitly[UDT[T4]].fieldTypes ++ implicitly[UDT[T5]].fieldTypes ++ implicitly[UDT[T6]].fieldTypes ++ implicitly[UDT[T7]].fieldTypes ++ implicitly[UDT[T8]].fieldTypes ++ implicitly[UDT[T9]].fieldTypes ++ implicitly[UDT[T10]].fieldTypes ++ implicitly[UDT[T11]].fieldTypes ++ implicitly[UDT[T12]].fieldTypes ++ implicitly[UDT[T13]].fieldTypes ++ implicitly[UDT[T14]].fieldTypes ++ implicitly[UDT[T15]].fieldTypes ++ implicitly[UDT[T16]].fieldTypes ++ implicitly[UDT[T17]].fieldTypes ++ implicitly[UDT[T18]].fieldTypes ++ implicitly[UDT[T19]].fieldTypes ++ implicitly[UDT[T20]].fieldTypes ++ implicitly[UDT[T21]].fieldTypes ++ implicitly[UDT[T22]].fieldTypes

  override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[R] {

    private val (indexMap1, rest1) = indexMap.splitAt(implicitly[UDT[T1]].numFields)
    private val (indexMap2, rest2) = rest1.splitAt(implicitly[UDT[T2]].numFields)
    private val (indexMap3, rest3) = rest2.splitAt(implicitly[UDT[T3]].numFields)
    private val (indexMap4, rest4) = rest3.splitAt(implicitly[UDT[T4]].numFields)
    private val (indexMap5, rest5) = rest4.splitAt(implicitly[UDT[T5]].numFields)
    private val (indexMap6, rest6) = rest5.splitAt(implicitly[UDT[T6]].numFields)
    private val (indexMap7, rest7) = rest6.splitAt(implicitly[UDT[T7]].numFields)
    private val (indexMap8, rest8) = rest7.splitAt(implicitly[UDT[T8]].numFields)
    private val (indexMap9, rest9) = rest8.splitAt(implicitly[UDT[T9]].numFields)
    private val (indexMap10, rest10) = rest9.splitAt(implicitly[UDT[T10]].numFields)
    private val (indexMap11, rest11) = rest10.splitAt(implicitly[UDT[T11]].numFields)
    private val (indexMap12, rest12) = rest11.splitAt(implicitly[UDT[T12]].numFields)
    private val (indexMap13, rest13) = rest12.splitAt(implicitly[UDT[T13]].numFields)
    private val (indexMap14, rest14) = rest13.splitAt(implicitly[UDT[T14]].numFields)
    private val (indexMap15, rest15) = rest14.splitAt(implicitly[UDT[T15]].numFields)
    private val (indexMap16, rest16) = rest15.splitAt(implicitly[UDT[T16]].numFields)
    private val (indexMap17, rest17) = rest16.splitAt(implicitly[UDT[T17]].numFields)
    private val (indexMap18, rest18) = rest17.splitAt(implicitly[UDT[T18]].numFields)
    private val (indexMap19, rest19) = rest18.splitAt(implicitly[UDT[T19]].numFields)
    private val (indexMap20, rest20) = rest19.splitAt(implicitly[UDT[T20]].numFields)
    private val (indexMap21, indexMap22) = rest20.splitAt(implicitly[UDT[T21]].numFields)

    private val inner1 = implicitly[UDT[T1]].createSerializer(indexMap1)
    private val inner2 = implicitly[UDT[T2]].createSerializer(indexMap2)
    private val inner3 = implicitly[UDT[T3]].createSerializer(indexMap3)
    private val inner4 = implicitly[UDT[T4]].createSerializer(indexMap4)
    private val inner5 = implicitly[UDT[T5]].createSerializer(indexMap5)
    private val inner6 = implicitly[UDT[T6]].createSerializer(indexMap6)
    private val inner7 = implicitly[UDT[T7]].createSerializer(indexMap7)
    private val inner8 = implicitly[UDT[T8]].createSerializer(indexMap8)
    private val inner9 = implicitly[UDT[T9]].createSerializer(indexMap9)
    private val inner10 = implicitly[UDT[T10]].createSerializer(indexMap10)
    private val inner11 = implicitly[UDT[T11]].createSerializer(indexMap11)
    private val inner12 = implicitly[UDT[T12]].createSerializer(indexMap12)
    private val inner13 = implicitly[UDT[T13]].createSerializer(indexMap13)
    private val inner14 = implicitly[UDT[T14]].createSerializer(indexMap14)
    private val inner15 = implicitly[UDT[T15]].createSerializer(indexMap15)
    private val inner16 = implicitly[UDT[T16]].createSerializer(indexMap16)
    private val inner17 = implicitly[UDT[T17]].createSerializer(indexMap17)
    private val inner18 = implicitly[UDT[T18]].createSerializer(indexMap18)
    private val inner19 = implicitly[UDT[T19]].createSerializer(indexMap19)
    private val inner20 = implicitly[UDT[T20]].createSerializer(indexMap20)
    private val inner21 = implicitly[UDT[T21]].createSerializer(indexMap21)
    private val inner22 = implicitly[UDT[T22]].createSerializer(indexMap22)

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
      implicitly[Product22Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, R]].create(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21, x22)
    }
  }
}