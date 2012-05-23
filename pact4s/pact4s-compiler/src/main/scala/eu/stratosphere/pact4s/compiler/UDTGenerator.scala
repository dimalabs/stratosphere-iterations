package eu.stratosphere.pact4s.compiler

import scala.reflect._
import scala.tools.nsc
import nsc.ast.TreeDSL
import nsc.symtab.Flags
import nsc.Global
import nsc.plugins.PluginComponent
import nsc.transform.Transform

trait UDTGenerator extends PluginComponent with UDTAnalyzer with Transform with TreeDSL {

  import global._
  import CODE._

  override val phaseName = "Pact4s.UDTGenerator"

  override def newTransformer(unit: CompilationUnit) = new UDTGeneratorTransformer(unit)

  class UDTGeneratorTransformer(unit: CompilationUnit) extends Transformer {

    private val unanalyzedUdt = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.package"), "unanalyzedUDT")

    override def transform(tree: Tree): Tree = {

      tree match {

        case TypeApply(s @ Select(_, _), List(t)) if s.symbol == unanalyzedUdt => {

          val typeName = t.tpe.typeSymbol.name + (t.tpe.typeArgs match {
            case List() => ""
            case args   => "[" + args.map(_.typeSymbol.name).mkString(", ") + "]"
          })

          analyzeType(t.tpe) match {
            case Left(err)     => unit.error(tree.pos, "Could not generate UDT[" + typeName + "]: " + err)
            case Right(result) => unit.warning(tree.pos, "Generating UDT[" + typeName + "]: " + result)
          }
        }

        case _ =>
      }

      super.transform(tree)
    }

    private def generateUDT(desc: UDTDescriptor) {

    }
  }
}
