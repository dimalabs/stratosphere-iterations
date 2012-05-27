package eu.stratosphere.pact4s.common.analyzer

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom

import eu.stratosphere.pact4s.common.udts._

import eu.stratosphere.pact.common.`type`.{ Key => PactKey }
import eu.stratosphere.pact.common.`type`.{ Value => PactValue }
import eu.stratosphere.pact.common.`type`.PactRecord

trait UDT[T] extends Serializable {

  val fieldTypes: Array[Class[_ <: PactValue]]
  def numFields = fieldTypes.length

  def getKeySet(fields: Seq[Int]): Array[Class[_ <: PactKey]] = {
    fields map { fieldNum => fieldTypes(fieldNum).asInstanceOf[Class[_ <: PactKey]] } toArray
  }

  def createSerializer(indexMap: Array[Int]): UDTSerializer[T]
}

abstract class UDTSerializer[T] extends Serializable {

  def serialize(item: T, record: PactRecord)
  def deserialize(record: PactRecord): T
}

trait UDTLowPriorityImplicits {

  class UDTAnalysisFailedException extends RuntimeException("UDT analysis failed. This should never happen.")

  implicit def unanalyzedUDT[T]: UDT[T] = throw new UDTAnalysisFailedException
}

object UDT extends UDTLowPriorityImplicits {

  implicit val booleanUdt = new BooleanUDT
  implicit val byteUdt = new ByteUDT
  implicit val charUdt = new CharUDT
  implicit val doubleUdt = new DoubleUDT
  implicit val intUdt = new IntUDT
  implicit val longUdt = new LongUDT
  implicit val shortUdt = new ShortUDT
  implicit val stringUdt = new StringUDT

  implicit def arrayUdt[T](implicit m: Manifest[T], udt: UDT[T]) = new ArrayUDT[T]
  implicit def listUdt[T, L[T] <: GenTraversableOnce[T]](implicit udt: UDT[T], bf: CanBuildFrom[GenTraversableOnce[T], T, L[T]]) = new ListUDT[T, L]

  import ProductFactories._

  /* REPL script to generate the implicits for Products: 
  object ProductImplicitGenerator {

    def generateImplicit(arity: Int) = {

      def mkList(template: Int => String) = (1 to arity) map template mkString (", ")

      val typeArgs = mkList { "T" + _ }
      "  implicit def product" + arity + "Udt[" + (mkList { "T" + _ + ": UDT" }) + "] = new Product" + arity + "UDT[" + typeArgs + ", Tuple" + arity + "[" + typeArgs + "]]"
    }

    println { (1 to 22) map { generateImplicit(_) } mkString ("\n") }
  }
  // */

  implicit def product1Udt[T1: UDT] = new Product1UDT[T1, Tuple1[T1]]
  implicit def product2Udt[T1: UDT, T2: UDT] = new Product2UDT[T1, T2, Tuple2[T1, T2]]
  implicit def product3Udt[T1: UDT, T2: UDT, T3: UDT] = new Product3UDT[T1, T2, T3, Tuple3[T1, T2, T3]]
  implicit def product4Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT] = new Product4UDT[T1, T2, T3, T4, Tuple4[T1, T2, T3, T4]]
  implicit def product5Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT] = new Product5UDT[T1, T2, T3, T4, T5, Tuple5[T1, T2, T3, T4, T5]]
  implicit def product6Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT] = new Product6UDT[T1, T2, T3, T4, T5, T6, Tuple6[T1, T2, T3, T4, T5, T6]]
  implicit def product7Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT] = new Product7UDT[T1, T2, T3, T4, T5, T6, T7, Tuple7[T1, T2, T3, T4, T5, T6, T7]]
  implicit def product8Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT] = new Product8UDT[T1, T2, T3, T4, T5, T6, T7, T8, Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]]
  implicit def product9Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT] = new Product9UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]]
  implicit def product10Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT] = new Product10UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]]
  implicit def product11Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT] = new Product11UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]]
  implicit def product12Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT] = new Product12UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]]
  implicit def product13Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT] = new Product13UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]]
  implicit def product14Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT] = new Product14UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]]
  implicit def product15Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT] = new Product15UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]]
  implicit def product16Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT] = new Product16UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]]
  implicit def product17Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT] = new Product17UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]]
  implicit def product18Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT] = new Product18UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]]
  implicit def product19Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT] = new Product19UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]]
  implicit def product20Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT] = new Product20UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]]
  implicit def product21Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, T21: UDT] = new Product21UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]]
  implicit def product22Udt[T1: UDT, T2: UDT, T3: UDT, T4: UDT, T5: UDT, T6: UDT, T7: UDT, T8: UDT, T9: UDT, T10: UDT, T11: UDT, T12: UDT, T13: UDT, T14: UDT, T15: UDT, T16: UDT, T17: UDT, T18: UDT, T19: UDT, T20: UDT, T21: UDT, T22: UDT] = new Product22UDT[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]]
}

