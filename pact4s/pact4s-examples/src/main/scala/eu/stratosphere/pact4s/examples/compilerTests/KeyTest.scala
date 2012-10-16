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

class KeyTest { 

  def toFS[T1: UDT, R](fun: FieldSelectorCode[T1 => R]) = fun

  final def fst(x: (Int, Int, Int)): Int = x._1
  final def snd(x: (Int, Int, Int)): Int = x._2
  final def fstAndSnd = (x: (Int, Int, Int)) => (x._1, x._2)
  final def sndAndFst = (x: (Int, Int, Int)) => (x._2, x._1)
  final def id[T](x: T): T = x

  case class IntPair(x: Int, y: Int = 0) {
    def this() = this(0, 0)
    final def getX = x
  }

  object IntPairExtr {
    def unapply(xy: IntPair): Some[(Int, Int)] = Some(xy.x, xy.y) // custom extractors fail :-(
  }

  abstract sealed class TestBase { val x: Int; val y: Int; def isOne = false; def isTwo = false }
  case class TestSub1(x: Int, y: Int) extends TestBase { override def isOne = true }
  case class TestSub2(x: Int, y: Int) extends TestBase { override def isTwo = true }
  
  val testID = toFS { x: (Int, Int, Int) => x }
  val testFunApp = toFS { arg: (Int, Int, Int) => sndAndFst(id(arg)) }
  val testThisExpr: FieldSelectorCode[IntPair => Int] = { arg: IntPair => arg.getX }
  
  val testPatMatch: FieldSelectorCode[((Int, (Int, Int))) => Any] = {
    case (x, (y, z)) =>
      val q = IntPair(x, z)
      var r = q
      //r = IntPair(0, 0) // This correctly breaks
      (r.y, r.x)
  }
  
  val testDefToFun = toFS { testUnapply _ }

  final def testUnapply(q: (Int, (Int, Int))) = {
    val (x, p @ (y, z)) = q
    val r = IntPair(y, z)
    r.x
    //r match { case IntPairExtr(_, z2) => z2 }
    //p match { case (y2, _) => (x, y2) }
    //val IntPairExtr(y2, z2) = r
    //z2
  }

  // testPingPong and testY correctly produce a "NonReducible(Recursive)" error
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
  
  /*
  val testPingPong = toFS { ping _ }
  val testY =  toFS { Y { rec2nd } }
  */

} 

