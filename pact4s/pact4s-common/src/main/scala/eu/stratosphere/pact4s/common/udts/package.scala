package eu.stratosphere.pact4s.common

package object udts {

  import ProductFactories._

  implicit def instTuple1[T1]: Product1Factory[T1, Tuple1[T1]] = new Product1Factory[T1, Tuple1[T1]] { def create(x1: T1): Tuple1[T1] = new Tuple1(x1) }
  implicit def instTuple2[T1, T2]: Product2Factory[T1, T2, Tuple2[T1, T2]] = new Product2Factory[T1, T2, Tuple2[T1, T2]] { def create(x1: T1, x2: T2): Tuple2[T1, T2] = new Tuple2(x1, x2) }
  implicit def instTuple3[T1, T2, T3]: Product3Factory[T1, T2, T3, Tuple3[T1, T2, T3]] = new Product3Factory[T1, T2, T3, Tuple3[T1, T2, T3]] { def create(x1: T1, x2: T2, x3: T3): Tuple3[T1, T2, T3] = new Tuple3(x1, x2, x3) }
  implicit def instTuple4[T1, T2, T3, T4]: Product4Factory[T1, T2, T3, T4, Tuple4[T1, T2, T3, T4]] = new Product4Factory[T1, T2, T3, T4, Tuple4[T1, T2, T3, T4]] { def create(x1: T1, x2: T2, x3: T3, x4: T4): Tuple4[T1, T2, T3, T4] = new Tuple4(x1, x2, x3, x4) }
  implicit def instTuple5[T1, T2, T3, T4, T5]: Product5Factory[T1, T2, T3, T4, T5, Tuple5[T1, T2, T3, T4, T5]] = new Product5Factory[T1, T2, T3, T4, T5, Tuple5[T1, T2, T3, T4, T5]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5): Tuple5[T1, T2, T3, T4, T5] = new Tuple5(x1, x2, x3, x4, x5) }
  implicit def instTuple6[T1, T2, T3, T4, T5, T6]: Product6Factory[T1, T2, T3, T4, T5, T6, Tuple6[T1, T2, T3, T4, T5, T6]] = new Product6Factory[T1, T2, T3, T4, T5, T6, Tuple6[T1, T2, T3, T4, T5, T6]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6): Tuple6[T1, T2, T3, T4, T5, T6] = new Tuple6(x1, x2, x3, x4, x5, x6) }
  implicit def instTuple7[T1, T2, T3, T4, T5, T6, T7]: Product7Factory[T1, T2, T3, T4, T5, T6, T7, Tuple7[T1, T2, T3, T4, T5, T6, T7]] = new Product7Factory[T1, T2, T3, T4, T5, T6, T7, Tuple7[T1, T2, T3, T4, T5, T6, T7]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7): Tuple7[T1, T2, T3, T4, T5, T6, T7] = new Tuple7(x1, x2, x3, x4, x5, x6, x7) }
  implicit def instTuple8[T1, T2, T3, T4, T5, T6, T7, T8]: Product8Factory[T1, T2, T3, T4, T5, T6, T7, T8, Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]] = new Product8Factory[T1, T2, T3, T4, T5, T6, T7, T8, Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8): Tuple8[T1, T2, T3, T4, T5, T6, T7, T8] = new Tuple8(x1, x2, x3, x4, x5, x6, x7, x8) }
  implicit def instTuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]: Product9Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = new Product9Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9): Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9] = new Tuple9(x1, x2, x3, x4, x5, x6, x7, x8, x9) }
  implicit def instTuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]: Product10Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]] = new Product10Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10): Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] = new Tuple10(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) }
  implicit def instTuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]: Product11Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]] = new Product11Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11): Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11] = new Tuple11(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11) }
  implicit def instTuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]: Product12Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]] = new Product12Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12): Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12] = new Tuple12(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12) }
  implicit def instTuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]: Product13Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]] = new Product13Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13): Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13] = new Tuple13(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13) }
  implicit def instTuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]: Product14Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]] = new Product14Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14): Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14] = new Tuple14(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14) }
  implicit def instTuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]: Product15Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]] = new Product15Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15): Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15] = new Tuple15(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15) }
  implicit def instTuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]: Product16Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]] = new Product16Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16): Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16] = new Tuple16(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16) }
  implicit def instTuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]: Product17Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]] = new Product17Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17): Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17] = new Tuple17(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17) }
  implicit def instTuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]: Product18Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]] = new Product18Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18): Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18] = new Tuple18(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18) }
  implicit def instTuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]: Product19Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]] = new Product19Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19): Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19] = new Tuple19(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19) }
  implicit def instTuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]: Product20Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]] = new Product20Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19, x20: T20): Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20] = new Tuple20(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20) }
  implicit def instTuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]: Product21Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]] = new Product21Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19, x20: T20, x21: T21): Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21] = new Tuple21(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21) }
  implicit def instTuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]: Product22Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]] = new Product22Factory[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]] { def create(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5, x6: T6, x7: T7, x8: T8, x9: T9, x10: T10, x11: T11, x12: T12, x13: T13, x14: T14, x15: T15, x16: T16, x17: T17, x18: T18, x19: T19, x20: T20, x21: T21, x22: T22): Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22] = new Tuple22(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19, x20, x21, x22) }

}