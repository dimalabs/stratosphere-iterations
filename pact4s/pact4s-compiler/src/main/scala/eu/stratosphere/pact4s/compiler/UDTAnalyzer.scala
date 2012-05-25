package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform

import eu.stratosphere.pact4s.compiler.util.Traverse

abstract class UDTAnalyzer(udtDescriptors: UDTDescriptors) extends PluginComponent with Traverse {

  override val global: udtDescriptors.global.type = udtDescriptors.global

  import global._
  import udtDescriptors._

  override val phaseName = "Pact4s.UDTAnalyzer"

  override def newTraverser(unit: CompilationUnit) = new Traverser {

    private val genSites = getGenSites(unit)
    private val genSitePaths = mutable.Map[UDTDescriptor, Seq[Tree]]()

    private val unanalyzedUdt = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.package"), "unanalyzedUDT")

    override def traverse(tree: Tree) = {

      tree match {

        case TypeApply(s @ Select(_, _), List(t)) if s.symbol == unanalyzedUdt => {

          getUDTDescriptor(t.tpe) match {
            case Left(err)     => unit.error(tree.pos, "Could not generate UDT[" + t.tpe + "]: " + err)
            case Right(result) => updateGenSite(result)
          }
        }

        case _ =>
      }

      super.traverse(tree)
    }

    private def updateGenSite(desc: UDTDescriptor) = {

      genSitePaths get desc match {

        case Some(oldPath) => {
          val newPath = findCommonLexicalParent(oldPath, getPath)
          genSites(oldPath.head) -= desc
          genSites(newPath.head) += desc
          genSitePaths(desc) = newPath
        }

        case None => {
          genSites(getPath.head) += desc
          genSitePaths(desc) = getPath
        }
      }
    }

    private def findCommonLexicalParent(path1: Seq[Tree], path2: Seq[Tree]): Seq[Tree] = {
      (path1.reverse, path2.reverse).zipped.takeWhile(p => p._1 == p._2).map(_._1).toSeq.reverse
    }
  }
}
