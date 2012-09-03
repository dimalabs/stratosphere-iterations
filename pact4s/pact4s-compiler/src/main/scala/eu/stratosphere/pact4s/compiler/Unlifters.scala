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

trait Unlifters { this: Pact4sPlugin =>

  import global._
  import defs._

  trait Unlifter extends Pact4sComponent {

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with Logger with TreeGenerator {

      override def apply(tree: Tree) = {

        super.apply {
          tree match {
            case Apply(t: TypeApply, _) if unanalyzed.contains(t.symbol) => applyDefaultUDF(unlift(tree))
            case _ => tree
          }
        }
      }

      private def unlift(tree: Tree): Tree = tree match {

        // <unanalyzedKind>Code[<tparams = fun.type>](Code.lift(<fun>))
        case Apply(TypeApply(Unlifted(kind), tparams), List(Apply(lift, List(fun: Function)))) if lift.symbol == liftMethod => {

          // fun.type => <kind>Code[fun.type]
          val viewTpe = mkCodeView(kind, tparams map { _.tpe })

          // Infer user supplied value or unlifted <unanalyzedKind>
          inferImplicitView(viewTpe, dontInfer = Set()) match {
            case None      => tree
            case Some(ref) => localTyper.typed { Apply(ref, List(fun)) setPos tree.pos }
          }
        }

        case _ => tree
      }

      private def applyDefaultUDF(tree: Tree): Tree = {

        val default = tree match {
          case Apply(TypeApply(unanalyzed, tparams @ List(_, _)), List(fun)) if unanalyzed.symbol == unanalyzedUDF1 => inferDefaultUDF(tparams map { _.tpe }, fun)
          case Apply(TypeApply(unanalyzed, tparams @ List(_, _, _)), List(fun)) if unanalyzed.symbol == unanalyzedUDF2 => inferDefaultUDF(tparams map { _.tpe }, fun)
          case _ => None
        }

        default map localTyper.typed getOrElse tree
      }

      private def inferDefaultUDF(tparams: List[Type], fun: Tree): Option[Tree] = {

        val funTpe = mkFunctionType(tparams: _*)
        val tpeudts = tparams.zip(tparams map { tpe => inferImplicitInst(mkUdtOf(unwrapIter(tpe))) })

        val errs = tpeudts filter { !_._2.isDefined } map { _._1 }
        val udts = tpeudts filter { _._2.isDefined } map { _._2.get }

        def analyzer = mkSelect("eu", "stratosphere", "pact4s", "common", "analyzer")
        def getDefaultUdf(owner: String, kind: String) = Apply(Apply(mkSelect(analyzer, owner, kind), List(fun)), udts)

        // TODO (Joe): Find a better way to figure out what kind of default UDF we want.
        // Going by whether the type signature includes an Iterator prevents supporting
        // Iterator as a List variant for UDT generation.
        def defaultUdf = (tparams: @unchecked) match {
          case List(t1, r) if isIter(t1) => getDefaultUdf("AnalyzedUDF1", "defaultIterT")
          case List(t1, r) if isIter(r) => getDefaultUdf("AnalyzedUDF1", "defaultIterR")
          case List(t1, r) => getDefaultUdf("AnalyzedUDF1", "default")
          case List(t1, t2, r) if isIter(t1) && isIter(t2) && isIter(r) => getDefaultUdf("AnalyzedUDF2", "defaultIterTR")
          case List(t1, t2, r) if isIter(t1) && isIter(t2) => getDefaultUdf("AnalyzedUDF2", "defaultIterT")
          case List(t1, t2, r) if isIter(r) => getDefaultUdf("AnalyzedUDF2", "defaultIterR")
          case List(t1, t2, r) => getDefaultUdf("AnalyzedUDF2", "default")
        }

        errs match {
          case Nil => Debug.report("Inferred default for UDF[" + funTpe + "] @ " + curTree.id); Some(defaultUdf)
          case _   => Error.report("Could not infer default UDF[" + funTpe + "] @ " + curTree.id + " Missing UDTs: " + errs.mkString(", ")); None
        }
      }
    }
  }
}