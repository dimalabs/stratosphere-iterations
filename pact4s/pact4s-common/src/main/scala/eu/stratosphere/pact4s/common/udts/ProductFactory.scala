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

/* REPL script to generate the contents of this file and the package object: 
object ProductFactoryGenerator {

  def generateProductFactory(arity: Int) = {

    def mkList(template: Int => String) = (1 to arity) map template mkString (", ")

    val typeArgs = mkList { "T" + _ }
    val funArgs = mkList { i => "x" + i + ": T" + i }

    val factory = "abstract class Product" + arity + "Factory[" + typeArgs + ", R <: Product" + arity + "[" + typeArgs + "]] { def create(" + funArgs + "): R }"
    val builder = "type ProductBuilder" + arity + "[" + typeArgs + "] = { type Factory[R <: Product" + arity + "[" + typeArgs + "]] = Product" + arity + "Factory[" + typeArgs + ", R] }"
    val inst = "implicit def instTuple" + arity + "[" + typeArgs + "]: Product" + arity + "Factory[" + typeArgs + ", Tuple" + arity + "[" + typeArgs + "]] = " +
      "new Product" + arity + "Factory[" + typeArgs + ", Tuple" + arity + "[" + typeArgs + "]] { def create(" + funArgs + "): Tuple" + arity + "[" + typeArgs + "] = new Tuple" + arity + "(" + (mkList { "x" + _ }) + ") }"

    (factory, builder, inst)
  }

  println {
    val (factories, builders, instances) = (1 to 22) map { generateProductFactory(_) } reduceLeft { (x, y) =>
      (x, y) match {
        case ((f1, b1, i1), (f2, b2, i2)) => (f1 + "\n" + f2, b1 + "\n" + b2, i1 + "\n" + i2)
      }
    }

    factories + "\n\n" + builders + "\n\n" + instances
  }
}
// */

object ProductFactories {

  abstract class Product1Factory[T1, R <: Product1[T1]] { def create(x1: T1): R }
  abstract class Product2Factory[T1, T2, R <: Product2[T1, T2]] { def create(x1: T1, x2: T2): R }
  abstract class Product3Factory[T1, T2, T3, R <: Product3[T1, T2, T3]] { def create(x1: T1, x2: T2, x3: T3): R }
  abstract class Product4Factory[T1, T2, T3, T4, R <: Product4[T1, T2, T3, T4]] { def create(x1: T1, x2: T2, x3: T3, x4: T4): R }
  abstract class Product5Factory[T1, T2, T3, T4, T5, R <: Product5[T1, T2, T3, T4, T5]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5): R }
  abstract class Product6Factory[T1, T2, T3, T4, T5, T6, R <: Product6[T1, T2, T3, T4, T5, T6]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6): R }
  abstract class Product7Factory[T1, T2, T3, T4, T5, T6, T7, R <: Product7[T1, T2, T3, T4, T5, T6, T7]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7): R }
  abstract class Product8Factory[T1, T2, T3, T4, T5, T6, T7, T8, R <: Product8[T1, T2, T3, T4, T5, T6, T7, T8]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8): R }
  abstract class Product9Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, R <: Product9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9): R }
  abstract class Product10Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R <: Product10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10): R }
  abstract class Product11Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R <: Product11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11): R }
  abstract class Product12Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R <: Product12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12): R }
  abstract class Product13Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R <: Product13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13): R }
  abstract class Product14Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R <: Product14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14): R }
  abstract class Product15Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R <: Product15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15): R }
  abstract class Product16Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R <: Product16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16): R }
  abstract class Product17Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R <: Product17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17): R }
  abstract class Product18Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R <: Product18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18): R }
  abstract class Product19Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R <: Product19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19): R }
  abstract class Product20Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R <: Product20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19, x20: T20): R }
  abstract class Product21Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, R <: Product21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19, x20: T20, x21: T21): R }
  abstract class Product22Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, R <: Product22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19, x20: T20, x21: T21, x22: T22): R }

  type ProductBuilder1[T1] = { type Factory[R <: Product1[T1]] = Product1Factory[T1, R] }
  type ProductBuilder2[T1, T2] = { type Factory[R <: Product2[T1, T2]] = Product2Factory[T1, T2, R] }
  type ProductBuilder3[T1, T2, T3] = { type Factory[R <: Product3[T1, T2, T3]] = Product3Factory[T1, T2, T3, R] }
  type ProductBuilder4[T1, T2, T3, T4] = { type Factory[R <: Product4[T1, T2, T3, T4]] = Product4Factory[T1, T2, T3, T4, R] }
  type ProductBuilder5[T1, T2, T3, T4, T5] = { type Factory[R <: Product5[T1, T2, T3, T4, T5]] = Product5Factory[T1, T2, T3, T4, T5, R] }
  type ProductBuilder6[T1, T2, T3, T4, T5, T6] = { type Factory[R <: Product6[T1, T2, T3, T4, T5, T6]] = Product6Factory[T1, T2, T3, T4, T5, T6, R] }
  type ProductBuilder7[T1, T2, T3, T4, T5, T6, T7] = { type Factory[R <: Product7[T1, T2, T3, T4, T5, T6, T7]] = Product7Factory[T1, T2, T3, T4, T5, T6, T7, R] }
  type ProductBuilder8[T1, T2, T3, T4, T5, T6, T7, T8] = { type Factory[R <: Product8[T1, T2, T3, T4, T5, T6, T7, T8]] = Product8Factory[T1, T2, T3, T4, T5, T6, T7, T8, R] }
  type ProductBuilder9[T1, T2, T3, T4, T5, T6, T7, T8, T9] = { type Factory[R <: Product9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = Product9Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] }
  type ProductBuilder10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] = { type Factory[R <: Product10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]] = Product10Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] }
  type ProductBuilder11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11] = { type Factory[R <: Product11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]] = Product11Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R] }
  type ProductBuilder12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12] = { type Factory[R <: Product12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]] = Product12Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R] }
  type ProductBuilder13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13] = { type Factory[R <: Product13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]] = Product13Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R] }
  type ProductBuilder14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14] = { type Factory[R <: Product14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]] = Product14Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R] }
  type ProductBuilder15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15] = { type Factory[R <: Product15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]] = Product15Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R] }
  type ProductBuilder16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16] = { type Factory[R <: Product16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]] = Product16Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R] }
  type ProductBuilder17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17] = { type Factory[R <: Product17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]] = Product17Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R] }
  type ProductBuilder18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18] = { type Factory[R <: Product18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]] = Product18Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R] }
  type ProductBuilder19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19] = { type Factory[R <: Product19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]] = Product19Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R] }
  type ProductBuilder20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20] = { type Factory[R <: Product20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]] = Product20Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R] }
  type ProductBuilder21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21] = { type Factory[R <: Product21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]] = Product21Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, R] }
  type ProductBuilder22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22] = { type Factory[R <: Product22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]] = Product22Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, R] }
}

