package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform

abstract class UDTGenerator(udtDescriptors: UDTDescriptors) extends PluginComponent with Transform {

  override val global: udtDescriptors.global.type = udtDescriptors.global

  import global._
  import udtDescriptors._

  override val phaseName = "Pact4s.UDTGenerator"

  override def newTransformer(unit: CompilationUnit) = new Transformer {

    private val genSites = getGenSites(unit)
    private val udtClasses = collection.mutable.Map[Type, (Symbol, UDTDescriptor)]()

    override def transform(tree: Tree): Tree = {

      for (udt <- genSites(tree)) {
        unit.warning(tree.pos, "Generating: UDT[" + udt.tpe + "]@" + tree.id + ":" + tree.pos.line + ":" + tree.pos.column)
      }

      super.transform(tree)
    }
  }
}