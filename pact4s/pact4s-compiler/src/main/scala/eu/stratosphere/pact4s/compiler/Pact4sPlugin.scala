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
  lazy val pactListClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDT.PactListImpl")
  lazy val pactIntegerClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")

  abstract sealed class UDTDescriptor { val id: Int; val tpe: Type }

  case class UnsupportedDescriptor(id: Int, tpe: Type, errors: Seq[String]) extends UDTDescriptor

  case class PrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapperClass: Symbol) extends UDTDescriptor

  case class ListDescriptor(id: Int, tpe: Type, listType: Type, bf: Tree, iter: Tree => Tree, elem: UDTDescriptor) extends UDTDescriptor {
    override def hashCode() = (id, tpe, listType, elem).hashCode()
    override def equals(that: Any) = that match {
      case ListDescriptor(thatId, thatTpe, thatListType, _, _, thatElem) => (id, tpe, listType, elem).equals((thatId, thatTpe, thatListType, thatElem))
      case _ => false
    }
  }

  case class BaseClassDescriptor(id: Int, tpe: Type, subTypes: Seq[UDTDescriptor]) extends UDTDescriptor

  case class CaseClassDescriptor(id: Int, tpe: Type, ctor: Symbol, ctorTpe: Type, getters: Seq[FieldAccessor]) extends UDTDescriptor {
    // Hack: ignore the ctorTpe, since two Type instances representing
    // the same ctor function type don't appear to be considered equal. 
    // Equality of the tpe and ctor fields implies equality of ctorTpe anyway.
    override def hashCode = (id, tpe, ctor, getters).hashCode
    override def equals(that: Any) = that match {
      case CaseClassDescriptor(thatId, thatTpe, thatCtor, _, thatGetters) => (id, tpe, ctor, getters).equals(thatId, thatTpe, thatCtor, thatGetters)
      case _ => false
    }
  }

  case class FieldAccessor(sym: Symbol, tpe: Type, descr: UDTDescriptor)

  case class OpaqueDescriptor(id: Int, tpe: Type, ref: Tree, rec: Boolean) extends UDTDescriptor {
    // Use string representation of Trees to approximate structural hashing and
    // equality, since Tree doesn't provide an implementation of these methods.
    override def hashCode() = (id, tpe, ref.toString, rec).hashCode()
    override def equals(that: Any) = that match {
      case OpaqueDescriptor(thatId, thatTpe, thatRef, thatRec) => (id, tpe, ref.toString, rec).equals((thatId, thatTpe, thatRef.toString, thatRec))
      case _ => false
    }
  }
}

