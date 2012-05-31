package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.TypingTransformers

import eu.stratosphere.pact4s.compiler.util._

trait UDTAnalysis { this: Pact4sGlobal =>

  import global._
  import Severity._

  private val genSites = MemoizedPartialFunction[CompilationUnit, Tree => mutable.Set[UDTDescriptor]]()
  override val getGenSites = genSites.liftWithDefaultDefine { _ => MemoizedPartialFunction[Tree, mutable.Set[UDTDescriptor]]() liftWithDefaultDefine { _ => mutable.Set() } }

  trait UDTAnalyzer extends PluginComponent with UDTMetaDataAnalyzer with Traverse with TypingTransformers {

    override val global: ThisGlobal = UDTAnalysis.this.global
    override val phaseName = "Pact4s.UDTAnalyzer"

    override def newTraverser(unit: CompilationUnit) = new TypingTransformer(unit) with Traverser {

      UDTAnalysis.this.messageTag = "Ana"
      private val genSites = getGenSites(unit)
      private val genSitePaths = MemoizedPartialFunction[UDTDescriptor, mutable.Set[Seq[Tree]]]() liftWithDefaultDefine { _ => mutable.Set() }

      override def isPathComponent(tree: Tree) = tree match {
        case _: ClassDef => true
        case _: Block    => true
        case _           => false
      }

      override def traverse(tree: Tree) = {

        currentPosition = tree.pos

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
        case BaseClassDescriptor(_, subTypes)      => subTypes flatMap { collectInferences(_) }
        case CaseClassDescriptor(_, _, _, getters) => getters flatMap { f => collectInferences(f.descr) }
      }

      private def updateGenSite(desc: UDTDescriptor) = {

        findCommonLexicalParent(currentPath, genSitePaths(desc).toSeq) match {

          case Some((oldPath, newPath)) => {
            genSites(oldPath.head) -= desc
            genSites(newPath.head) += desc
            genSitePaths(desc) -= oldPath
            genSitePaths(desc) += newPath
            log(Debug, newPath.head.pos) { "Updated GenSite[" + desc.tpe + "] " + oldPath.head.pos.line + ":" + oldPath.head.pos.column + " -> " + newPath.head.pos.line + ":" + newPath.head.pos.column }
          }

          case None => {
            genSites(currentPath.head) += desc
            genSitePaths(desc) += currentPath
            log(Debug, currentPath.head.pos) { "Added GenSite[" + desc.tpe + "] " + currentPath.head.pos.line + ":" + currentPath.head.pos.column }
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