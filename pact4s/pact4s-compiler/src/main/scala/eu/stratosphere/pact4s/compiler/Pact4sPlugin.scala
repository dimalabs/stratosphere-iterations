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

package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.Plugin
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.TypingTransformers

import eu.stratosphere.pact4s.compiler.util._

class Pact4sPlugin(val global: Global) extends Plugin with Pact4sGlobal {

  import global._

  val thisGlobal: ThisGlobal = global

  val thisGenSites = {
    val initial = mutable.Map[CompilationUnit, MutableMultiMap[Tree, UDTDescriptor]]()
    initial withDefault { unit =>
      val unitGenSites = new MutableMultiMap[Tree, UDTDescriptor]()
      initial(unit) = unitGenSites
      unitGenSites
    }
  }

  object udtGenSiteSelector extends UDTGenSiteSelector {
    override val global: ThisGlobal = thisGlobal
    override val genSites = thisGenSites
    override val runsAfter = List[String]("refchecks");
    override val runsRightAfter = Some("refchecks")
  }

  object udtCodeGen extends UDTCodeGenerator {
    override val global: ThisGlobal = thisGlobal
    override val genSites = thisGenSites
    override val runsAfter = List[String]("Pact4s.UDTGenSiteSelection");
    override val runsRightAfter = Some("Pact4s.UDTGenSiteSelection")
  }

  override val name = "Pact4s"
  override val description = "Performs analysis and code generation for Pact4s programs."
  override val components = List[PluginComponent](udtGenSiteSelector, udtCodeGen)
}

trait Pact4sGlobal extends TypingTransformers with Traversers with UDTAnalysis with UDTGenSiteSelection with UDTCodeGeneration with Logger {

  val global: Global
  type ThisGlobal = Pact4sGlobal.this.global.type

  import global._

  lazy val unanalyzedUdt = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDT"), "unanalyzedUDT")
  lazy val udtClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDT")
  lazy val udtSerializerClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDTSerializer")
  lazy val pactRecordClass = definitions.getClass("eu.stratosphere.pact.common.type.PactRecord")
  lazy val pactValueClass = definitions.getClass("eu.stratosphere.pact.common.type.Value")
  lazy val pactListClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactList")
  lazy val pactIntegerClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")

  abstract sealed class UDTDescriptor { val tpe: Type }

  case class UnsupportedDescriptor(tpe: Type, errors: Seq[String]) extends UDTDescriptor
  case class PrimitiveDescriptor(tpe: Type, default: Literal, wrapperClass: Symbol) extends UDTDescriptor
  case class ListDescriptor(tpe: Type, listType: Type, elem: UDTDescriptor) extends UDTDescriptor
  case class BaseClassDescriptor(tpe: Type, subTypes: Seq[UDTDescriptor]) extends UDTDescriptor

  case class CaseClassDescriptor(tpe: Type, ctor: Symbol, ctorTpe: Type, getters: Seq[FieldAccessor]) extends UDTDescriptor {
    // Hack: ignore the ctorTpe, since two Type instances representing
    // the same ctor function type don't appear to be considered equal. 
    // Equality of the tpe and ctor fields implies equality of ctorTpe anyway.
    override def hashCode = (tpe, ctor, getters).hashCode
    override def equals(that: Any) = that match {
      case CaseClassDescriptor(thatTpe, thatCtor, _, thatGetters) => (tpe, ctor, getters).equals(thatTpe, thatCtor, thatGetters)
      case _ => false
    }
  }

  case class FieldAccessor(sym: Symbol, tpe: Type, descr: UDTDescriptor)

  case class RecursiveDescriptor(tpe: Type, unpack: () => UDTDescriptor) extends UDTDescriptor {
    // Use the string representation of the unpacked descriptor to
    // approximate structural hashing and equality without triggering
    // an endless recursion.
    override def hashCode = (tpe, unpack().toString).hashCode
    override def equals(that: Any) = that match {
      case RecursiveDescriptor(thatTpe, thatUnpack) => (tpe, unpack().toString).equals((thatTpe, thatUnpack().toString))
      case _                                        => false
    }
  }

  case class OpaqueDescriptor(tpe: Type, ref: Tree) extends UDTDescriptor {
    // Use string representation of Trees to approximate structural hashing and
    // equality, since Tree doesn't provide an implementation of these methods.
    override def hashCode() = (tpe, ref.toString).hashCode()
    override def equals(that: Any) = that match {
      case OpaqueDescriptor(thatTpe, thatRef) => (tpe, ref.toString).equals((thatTpe, thatRef.toString))
      case _                                  => false
    }
  }
}

