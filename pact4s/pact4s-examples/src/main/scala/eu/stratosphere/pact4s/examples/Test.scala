package eu.stratosphere.pact4s.examples

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.operators._

abstract class Test extends PactProgram with TestGeneratedImplicits {

  /*
  val udtTestInst1 = {
    val x = implicitly[analyzer.UDT[(Int, Int, (String, Array[Int]))]]
    val y = new DataSource("", DelimetedDataSourceFormat({ s: String => (s, s, (s.toInt, s)) }))
    implicitly[analyzer.UDT[(Int, Int, (Int, String))]]
    val z = new DataSource("", DelimetedDataSourceFormat({ s: String => (s, s, (s, s)) }))
    implicitly[analyzer.UDT[(Int, Int, (Int, Int))]]
    implicitly[analyzer.UDT[(Int, Int, (String, String))]]
  }

  val udtTestInst2 = new DataSource("", DelimetedDataSourceFormat({ s: String => (s, s, (s.toInt, s)) }))
  val udtTestInst3 = implicitly[analyzer.UDT[(Int, Int, (Int, String))]]
  val udtTestInst4 = implicitly[analyzer.UDT[(Int, Int, (Int, Option[Int]))]]
  */

  //val udtTestInst = implicitly[analyzer.UDT[(String, (Int, Int, String))]]

  case class Point(x: Int, y: Int)
  val pointUdt1 = implicitly[UDT[Point]]
  val pointUdt2 = implicitly[UDT[Point]]
}

trait TestGeneratedImplicits { this: Test =>

  val pointUdt3 = implicitly[UDT[Point]]
  val pointUdt4 = implicitly[UDT[Point]]
}