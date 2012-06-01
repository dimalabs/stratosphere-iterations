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

  abstract sealed class UDTDescriptor { val tpe: Type }
  case class UnsupportedDescriptor(tpe: Type, errors: Seq[String]) extends UDTDescriptor
  case class OpaqueDescriptor(tpe: Type, ref: Tree) extends UDTDescriptor
  case class PrimitiveDescriptor(tpe: Type, default: Literal, wrapperClass: Symbol) extends UDTDescriptor
  case class ListDescriptor(tpe: Type, listType: Type, elem: UDTDescriptor) extends UDTDescriptor
  case class BaseClassDescriptor(tpe: Type, subTypes: Seq[UDTDescriptor]) extends UDTDescriptor
  case class CaseClassDescriptor(tpe: Type, ctor: Symbol, ctorTpe: Type, getters: Seq[FieldAccessor]) extends UDTDescriptor
  case class FieldAccessor(sym: Symbol, tpe: Type, descr: UDTDescriptor)
}
