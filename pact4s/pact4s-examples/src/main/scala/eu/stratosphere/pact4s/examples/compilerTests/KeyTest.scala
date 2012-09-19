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

package eu.stratosphere.pact4s.examples.compilerTests

import eu.stratosphere.pact4s.common.analyzer._

object FooTest {
  
  var _x: Int = 0
  
  def x = _x
  def x_=(value: Int) = _x = value
  
  x = 1
}

class KeyTest {

  FooTest.x = 2
  
  /*
  def toFS[T1: UDT, R](fun: FieldSelectorCode[T1 => R]) = fun

  final def fst(x: (Int, Int, Int)): Int = x._1
  final def snd(x: (Int, Int, Int)): Int = x._2
  final def id[T](x: T): T = x

  case class IntPair(x: Int, y: Int = 0) {
    def this() = this(0, 0)
    final def getX = x // This expressions fail :-(
  }

  object IntPairExtr {
    def unapply(xy: IntPair): Some[(Int, Int)] = Some(xy.x, xy.y) // custom extractors fail :-(
  }

  val test1: FieldSelectorCode[((Int, Int, Int)) => Int] = { arg => id(fst(arg)) }
  val test2: FieldSelectorCode[((Int, (Int, Int))) => Any] = {
    case (x, (y, z)) =>
      val q = IntPair(x, z)
      var r = q
      r = IntPair(0, 0) // Block statements aren't properly handled yet, especially in regard to assignment
      r.x
  }
  val test3 = toFS { x: (Int, Int, Int) => x }
  val test4 = toFS { testUnapply _ }

  final def testUnapply(q: (Int, (Int, Int))) = {
    val (x, p @ (y, z)) = q
    val r = IntPair(y, z)
    r.x
    //r match { case IntPairExtr(_, z2) => z2 }
    //p match { case (y2, _) => (x, y2) }
    //val IntPairExtr(y2, z2) = r
    //z2
  }
  */

  // These tests correctly produce a "Recursion detected" error
  /*
  final def ping(x: Int): Int = pong(x)
  final def pong(x: Int): Int = ping(x)

  final def Y[A, R](f: (A => R) => (A => R)): A => R = {
    case class Rec(f: Rec => (A => R));
    //(Rec { r => a => f(r.f(r))(a) }).f(Rec { r => a => f(r.f(r))(a) })
    val rec = Rec { r => a => f(r.f(r))(a) }
    rec.f(rec)
  }
  
  case class RecPair(_1: Int, _2: RecPair)
  final def rec2nd(f: RecPair => RecPair)(x: RecPair): RecPair = f(x._2)
  
  val testY =  toFS { Y { rec2nd } }
  val testPingPong = toFS { x: (Int, (Int, Int)) => ping(id(x)._2._1) }
  */

} 

