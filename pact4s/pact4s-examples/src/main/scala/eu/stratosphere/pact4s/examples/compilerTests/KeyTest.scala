package eu.stratosphere.pact4s.examples.compilerTests

import eu.stratosphere.pact4s.common.analyzer._

class KeyTest {

  def toFS[T1: UDT, R](fun: FieldSelectorCode[T1 => R]) = fun

  def fst(x: (Int, Int, Int)): Int = x._1
  def snd(x: (Int, Int, Int)): Int = x._2
  def id[T](x: T): T = x
  def ping(x: Int): Int = pong(x)
  def pong(x: Int): Int = ping(x)

  case class IntPair(x: Int, y: Int) {
    def this() = this(0, 0)
  }

  object IntPairExtr {
    def unapply(xy: IntPair): Some[(Int, Int)] = Some(xy.x, xy.y)
  }

  val test1: FieldSelectorCode[((Int, Int, Int)) => Int] = { arg => fst(arg) }
  val test2: FieldSelectorCode[((Int, (Int, Int))) => Any] = { case (x, (y, z)) =>
    val q = IntPair(x, z)
    q.y
  } 
  val test3 = toFS { x: (Int, Int, Int) => x }
  val test4 = toFS { testUnapply _ }

  def testUnapply(q: (Int, (Int, Int))) = {
    val (x, p @ (y, z)) = q
    val r = IntPair(y, z)
    r.x
    //r match { case IntPairExtr(_, z2) => z2 }
    //p match { case (y2, _) => (x, y2) }
    //val IntPairExtr(y2, z2) = r
    //z2
  }

  val testSeq = Seq[String]("fst", "snd")
} 

