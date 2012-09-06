package eu.stratosphere.pact4s.examples.compilerTests

import eu.stratosphere.pact4s.common.analyzer._

class KeyTest {

  def toFS[T1: UDT, R](fun: FieldSelectorCode[T1 => R]) = fun
  
  def fst(x: (Int, Int, Int)): Int = x._1
  def snd(x: (Int, Int, Int)): Int = x._2
  def id[T](x: T): T = x
  def ping(x: Int): Int = pong(x)
  def pong(x: Int): Int = ping(x)
  
  case class IntPair(x: Int, y: Int)
  
  //val test1: FieldSelectorCode[((Int, Int, Int)) => Int] = { arg => fst(arg) }
  val test2: FieldSelectorCode[((Int, (Int, Int))) => Any] = { case (x, (y, z)) => IntPair(x, y) } 
  //val test3 = toFS { x: (Int, Int, Int) => x._1 }
  
  def testPat(xy: (Int, Int)) = xy match {
    case (x, y) => Some(x)
    case _ => None
  }
  
} //