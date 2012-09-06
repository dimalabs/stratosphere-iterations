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

package eu.stratosphere.pact4s.compiler.udt

import eu.stratosphere.pact4s.compiler.Pact4sPlugin
import eu.stratosphere.pact4s.compiler.util.MutableMultiMap

trait UDTGenSiteSelectors { this: Pact4sPlugin =>

  import global._

  trait UDTGenSiteSelector extends Pact4sComponent with UDTGenSiteParticipant {

    override def newTransformer(unit: CompilationUnit) = new TypingTraverser(unit) with Logger with UDTAnalyzer {

      private val genSites = getUDTGenSites(unit)
      private val genSitePaths = new MutableMultiMap[UDTDescriptor, Seq[Tree]]()

      override def apply(tree: Tree) = {

        tree match {

          // Analyze unanalyzed Udts
          case TypeApply(s: Select, List(t)) if s.symbol == defs.unanalyzedUdt => analyze(t.tpe)

          // Analyze UDF type parameters
          case Apply(TypeApply(unanalyzed, tps @ List(t1, r)), List(_)) if unanalyzed.symbol == defs.unanalyzedFieldSelector => analyze(t1.tpe)
          case Apply(TypeApply(unanalyzed, tps @ List(t1, r)), List(_)) if unanalyzed.symbol == defs.unanalyzedFieldSelectorCode => analyze(t1.tpe)
          case Apply(TypeApply(unanalyzed, tps @ List(t1, r)), List(_)) if unanalyzed.symbol == defs.unanalyzedUDF1 => analyze(tps)
          case Apply(TypeApply(unanalyzed, tps @ List(t1, r)), List(_)) if unanalyzed.symbol == defs.unanalyzedUDF1Code => analyze(tps)
          case Apply(TypeApply(unanalyzed, tps @ List(t1, t2, r)), List(_)) if unanalyzed.symbol == defs.unanalyzedUDF2 => analyze(tps)
          case Apply(TypeApply(unanalyzed, tps @ List(t1, t2, r)), List(_)) if unanalyzed.symbol == defs.unanalyzedUDF2Code => analyze(tps)

          case _ =>
        }

        super.apply(tree)
      }

      private def analyze(tpe: Type): Unit = getUDTDescriptor(tpe) match {
        case UnsupportedDescriptor(_, _, errs) => errs foreach { err => Error.report("Could not generate UDT[" + tpe + "]: " + err) }
        case d @ OpaqueDescriptor(_, _, _)     => collectInferences(d) foreach { apply(_) }
        case desc                              => if (updateGenSite(desc)) collectInferences(desc) foreach { apply(_) }
      }

      private def analyze(tparams: List[Tree]): Unit = tparams foreach { t => analyze(defs.unwrapIter(t.tpe)) }

      private def collectInferences(desc: UDTDescriptor): Seq[Tree] = desc match {
        case OpaqueDescriptor(_, _, ref)              => Seq(ref())
        case ListDescriptor(_, _, _, _, _, elem)      => collectInferences(elem)
        case BaseClassDescriptor(_, _, _, subTypes)   => subTypes flatMap { collectInferences(_) }
        case CaseClassDescriptor(_, _, _, _, getters) => getters flatMap { f => collectInferences(f.desc) }
        case _                                        => Seq()
      }

      private def curSitePath = curPath filter {
        case _: Block    => true
        case _: Function => true
        case _: DefDef   => true
        case _: ClassDef => true
        case _           => false
      }

      private def updateGenSite(desc: UDTDescriptor): Boolean = {

        findCommonLexicalParent(curSitePath, genSitePaths(desc).toSeq) match {

          case Some((oldPath, newPath)) if oldPath.head eq newPath.head => false

          case Some((oldPath, newPath)) => {
            verbosely[Boolean] { _ => "Updated GenSite[" + desc.tpe + "] " + posString(oldPath.head.pos) + " -> " + posString(newPath.head.pos) } {
              genSites(oldPath.head) -= desc
              genSites(newPath.head) += desc
              genSitePaths(desc) -= oldPath
              genSitePaths(desc) += newPath
              true
            }
          }

          case None => {
            verbosely[Boolean] { _ => "Added GenSite[" + desc.tpe + "] " + posString(curSitePath.head.pos) + " - " + desc.toString } {
              genSites(curSitePath.head) += desc
              genSitePaths(desc) += curSitePath
              true
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

