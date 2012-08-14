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

package eu.stratosphere.pact4s.examples

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analyzer._
import eu.stratosphere.pact4s.common.operators._

import eu.stratosphere.pact.common.`type`._
import eu.stratosphere.pact.common.`type`.base._

import scala.collection.JavaConversions._

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

  abstract sealed class Foo
  case class Bar(x: Int, y: Long, z: Array[Long]) extends Foo
  case class Baz(x: Int, y: Long, z: Foo) extends Foo

  case class Thing(x: Int, y: Long, f: Foo)

  val barUdt1 = implicitly[UDT[Bar]]
  //val bazUdt1 = implicitly[UDT[Baz]]
  //val fooUdt1 = implicitly[UDT[Foo]]
  //val barUdt2 = implicitly[UDT[Bar]]
  //val bazUdt2 = implicitly[UDT[Baz]]
  //val fooUdt2 = implicitly[UDT[Foo]]
  //val thingUdt1 = implicitly[UDT[Thing]]
  //val thingUdt2 = implicitly[UDT[Thing]]
  //val optBarUdt1 = implicitly[UDT[Option[Baz]]]
  //val optBarUdt2 = implicitly[UDT[Option[Baz]]]

  var pactList = new PactList[PactLong]() {}

  private def testArray[T](v: Array[T]) = {

    val iter = v.iterator
    while (iter.hasNext)
      println(iter.next)
  }

  private def testSeq[T](v: scala.collection.TraversableOnce[T]) = {
    val iter = v.toIterator
    while (iter.hasNext)
      println(iter.next)
  }

} //

trait TestGeneratedImplicits { this: Test =>

  /*
  val barUdt3 = implicitly[UDT[Bar]]
  val bazUdt3 = implicitly[UDT[Baz]]
  val fooUdt3 = implicitly[UDT[Foo]]
  val barUdt4 = implicitly[UDT[Bar]]
  val bazUdt4 = implicitly[UDT[Baz]]
  val fooUdt4 = implicitly[UDT[Foo]]
  */
}