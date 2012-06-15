package eu.stratosphere.pact4s.compiler

import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform

import eu.stratosphere.pact4s.compiler.util._

trait UDTGenSiteSelection { this: Pact4sGlobal =>

  import global._

  trait UDTGenSiteSelector extends PluginComponent with Transform {

    override val global: ThisGlobal = UDTGenSiteSelection.this.global
    override val phaseName = "Pact4s.UDTGenSiteSelection"

    val genSites: collection.Map[CompilationUnit, MutableMultiMap[Tree, UDTDescriptor]]

    override def newTransformer(unit: CompilationUnit) = new TypingTraverser(unit) with UDTAnalyzer {

      UDTGenSiteSelection.this.messageTag = "UDTSite"
      private val genSites = UDTGenSiteSelector.this.genSites(unit)
      private val genSitePaths = new MutableMultiMap[UDTDescriptor, Seq[Tree]]()

      override def isPathComponent(tree: Tree) = tree match {
        case _: ClassDef => true
        case _: Block    => true
        case _           => false
      }

      override def traverse(tree: Tree) = {

        currentPosition = tree.pos

        tree match {

          case TypeApply(s: Select, List(t)) if s.symbol == unanalyzedUdt => {

            getUDTDescriptor(t.tpe, tree) match {
              case UnsupportedDescriptor(_, errs) => errs foreach { err => log(Error) { "Could not generate UDT[" + t.tpe + "]: " + err } }
              case descr                          => updateGenSite(descr); collectInferences(descr) foreach { traverse(_) }
            }
          }

          case _ =>
        }

        super.traverse(tree)
      }

      private def collectInferences(descr: UDTDescriptor): Seq[Tree] = descr match {
        case OpaqueDescriptor(_, ref)              => Seq(ref)
        case PrimitiveDescriptor(_, _, _)          => Seq()
        case ListDescriptor(_, _, elem)            => collectInferences(elem)
        case BaseClassDescriptor(_, subTypes)      => subTypes flatMap { collectInferences(_) }
        case CaseClassDescriptor(_, _, _, getters) => getters flatMap { f => collectInferences(f.descr) }
      }

      private def updateGenSite(desc: UDTDescriptor) = {

        findCommonLexicalParent(currentPath, genSitePaths(desc).toSeq) match {

          case Some((oldPath, newPath)) => {
            verbosely[Unit] { _ => "Updated GenSite[" + desc.tpe + "] " + oldPath.head.pos.line + ":" + oldPath.head.pos.column + " -> " + newPath.head.pos.line + ":" + newPath.head.pos.column } {
              genSites(oldPath.head) -= desc
              genSites(newPath.head) += desc
              genSitePaths(desc) -= oldPath
              genSitePaths(desc) += newPath
            }
          }

          case None => {
            verbosely[Unit] { _ => "Added GenSite[" + desc.tpe + "] " + currentPath.head.pos.line + ":" + currentPath.head.pos.column } {
              genSites(currentPath.head) += desc
              genSitePaths(desc) += currentPath
            }
          }
        }
      }

      private def findCommonLexicalParent(path: Seq[Tree], candidates: Seq[Seq[Tree]]): Option[(Seq[Tree], Seq[Tree])] = {

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
    }
  }
}
