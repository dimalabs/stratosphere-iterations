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
    private val genSitePaths = mutable.Map[UDTDescriptor, Set[Seq[Tree]]]() withDefaultValue Set()

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

      genSitePaths get desc flatMap { findCommonLexicalParent(getPath, _) } match {

        case Some((oldPath, newPath)) => {
          genSites(oldPath.head) -= desc
          genSites(newPath.head) += desc
          genSitePaths(desc) -= oldPath
          genSitePaths(desc) += newPath
        }

        case None => {
          genSites(getPath.head) += desc
          genSitePaths(desc) += getPath
        }
      }
    }

    private def findCommonLexicalParent(path: Seq[Tree], candidates: Set[Seq[Tree]]): Option[(Seq[Tree], Seq[Tree])] = {

      val commonPaths =
        candidates.toSeq flatMap { candidate =>
          val commonPath = (path.reverse, candidate.reverse).zipped takeWhile { case (x, y) => x == y } map { _._1 } toSeq;
          if (commonPath.nonEmpty)
            Some((candidate, commonPath.reverse))
          else
            None
        }

      commonPaths match {
        case Seq(x) => Some(x)
        case Seq()  => None
      }
    }

    override def isPathComponent(tree: Tree) = tree match {
      case _: Template => true
      case _: Block    => true
      case _           => false
    }
  }
}
