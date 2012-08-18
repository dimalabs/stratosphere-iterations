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
  case class Bar(x: Int, y: Long, z: Seq[Baz], zz: Baz) extends Foo
  case class Baz(x: Int, y: Long, z: Array[(Int, Long)]) extends Foo

  case class Rec(x: Int, y: Long, f: Foo, g: Seq[Foo]) extends Foo

  case class Thing[T](x: Long, y: Long, z: T)

  abstract sealed class Simple
  case class B(b: Long, ba: A) extends Simple
  case class A(a: Long, ab: B) extends Simple

  val fooUdt1 = implicitly[UDT[Foo]]
  val fooUdt2 = implicitly[UDT[Foo]]
  val barUdt1 = implicitly[UDT[Bar]]
  val barUdt2 = implicitly[UDT[Bar]]
  val bazUdt1 = implicitly[UDT[Baz]]
  val bazUdt2 = implicitly[UDT[Baz]]
  val recUdt1 = implicitly[UDT[Rec]]
  val recUdt2 = implicitly[UDT[Rec]]
  val optBarUdt1 = implicitly[UDT[Option[Bar]]]
  val optBarUdt2 = implicitly[UDT[Option[Bar]]]
  val optFooUdt1 = implicitly[UDT[Option[Foo]]]
  val optFooUdt2 = implicitly[UDT[Option[Foo]]]

}

trait TestGeneratedImplicits { this: Test =>

  val testUdt = implicitly[UDT[Simple]]
  val fooUdt3 = implicitly[UDT[Foo]]
  val fooUdt4 = implicitly[UDT[Foo]]
  val barUdt3 = implicitly[UDT[Bar]]
  val barUdt4 = implicitly[UDT[Bar]]
  val bazUdt3 = implicitly[UDT[Baz]]
  val bazUdt4 = implicitly[UDT[Baz]]
  val recUdt3 = implicitly[UDT[Rec]]
  val recUdt4 = implicitly[UDT[Rec]]
  val optBarUdt3 = implicitly[UDT[Option[Bar]]]
  val optBarUdt4 = implicitly[UDT[Option[Bar]]]
  val optFooUdt3 = implicitly[UDT[Option[Foo]]]
  val optFooUdt4 = implicitly[UDT[Option[Foo]]]

}

class TestParent {
  abstract sealed class Foo
  case class B(b: Long, ba: A) extends Foo
  case class A(a: Long, ab: B) extends Foo
  val udtParent = implicitly[UDT[Foo]]
}

class TestChild extends TestParent {
  val udtChild = implicitly[UDT[Foo]]
} //

class Outer {
  case class Test[S, T](inner: S, outer: T)
  case class X(ov: Long, ox: X)

  class Inner {
    type Y = Outer.this.X
    case class Test[S, T](inner: S, outer: T)
    case class X(iv: Long, ix: X, iy: Y)

    private val plainUdt = implicitly[UDT[Test[X, Outer.this.X]]]
    private val aliasUdt = implicitly[UDT[Test[X, Y]]]

  }

  class Sub extends Inner {
    type Z = super.X

    private val innerUdt = implicitly[UDT[Test[X, Y]]]
    private val outerUdt = implicitly[UDT[Outer.this.Test[Y, Z]]]
  }
}
