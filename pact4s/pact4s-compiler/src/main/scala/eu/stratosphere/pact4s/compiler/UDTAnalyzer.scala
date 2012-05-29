package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

import eu.stratosphere.pact4s.compiler.util.Traverse
import eu.stratosphere.pact4s.compiler.util.Logger

abstract class UDTAnalyzer(udtDescriptors: UDTDescriptors) extends PluginComponent with Traverse with TypingTransformers {

  override val global: udtDescriptors.global.type = udtDescriptors.global

  import global._

  override val phaseName = "Pact4s.UDTAnalyzer"

  override def newTraverser(unit: CompilationUnit) = new TypingTransformer(unit) with Traverser {

    import udtDescriptors._
    import udtDescriptors.Severity._

    private val genSites = getGenSites(unit)
    private val genSitePaths = mutable.Map[UDTDescriptor, Set[Seq[Tree]]]() withDefaultValue Set()

    override def traverse(tree: Tree) = {

      udtDescriptors.curPos = tree.pos

      tree match {

        case TypeApply(s: Select, List(t)) if s.symbol == unanalyzedUdt => {

          analyzeUDT(t.tpe, infer(tree)) match {
            case UnsupportedDescriptor(_, errs) => errs foreach { err => log(Error) { "Could not generate UDT[" + t.tpe + "]: " + err } }
            case descr                          => updateGenSite(descr); collectInferences(descr) foreach { traverse(_) }
          }
        }

        case _ =>
      }

      super.traverse(tree)
    }

    private def infer(tree: Tree)(tpe: Type) = analyzer.inferImplicit(tree, appliedType(udtClass.tpe, List(tpe)), true, false, localTyper.context).tree

    private def collectInferences(descr: UDTDescriptor): Seq[Tree] = descr match {
      case OpaqueDescriptor(_, ref)              => Seq(ref)
      case PrimitiveDescriptor(_, _, _)          => Seq()
      case ListDescriptor(_, _, elem)            => collectInferences(elem)
      case CaseClassDescriptor(_, _, _, getters) => getters flatMap { f => collectInferences(f.descr) }
    }

    private def updateGenSite(desc: UDTDescriptor) = {

      genSitePaths get desc flatMap { findCommonLexicalParent(getPath, _) } match {

        case Some((oldPath, newPath)) => {
          genSites(oldPath.head) -= desc
          genSites(newPath.head) += desc
          genSitePaths(desc) -= oldPath
          genSitePaths(desc) += newPath
          log(Debug, newPath.head.pos) { "Updated GenSite[" + desc.tpe + "] " + oldPath.head.pos.line + ":" + oldPath.head.pos.column + " -> " + newPath.head.pos.line + ":" + newPath.head.pos.column }
        }

        case None => {
          genSites(getPath.head) += desc
          genSitePaths(desc) += getPath
          log(Debug, getPath.head.pos) { "Added GenSite[" + desc.tpe + "] " + getPath.head.pos.line + ":" + getPath.head.pos.column }
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
      case _: ClassDef => true
      case _: Block    => true
      case _           => false
    }
  }
}
