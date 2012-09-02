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
          val t1 = tree match {
            case UnanalyzedCode(unlifted) => localTyper.typed { unlifted }
            case _                        => tree
          }
          val t2 = t1 match {
            case UnanalyzedUDF(default) => localTyper.typed { default }
            case _                      => t1
          }
          t2 match {
            case UnanalyzedFunction(kind, tps) => Error.report("Found " + kind + "[" + tps.mkString(", ") + "] @ " + posString(tree.pos))
            case _                             => ()
          }
          t2
        }
      }

      object UnanalyzedCode {

        def unapply(tree: Tree): Option[Tree] = tree match {
          case Apply(TypeApply(view, tparams), List(Apply(lift, List(fun: Function)))) if lift.symbol == liftMethod => view.symbol match {
            case LiftedView(unliftedView @ Select(_, kind)) => inferImplicitView(mkCodeView(kind.toString, tparams map { _.tpe })) match {
              case None    => Some(Apply(TypeApply(unliftedView, tparams), List(fun)) setPos tree.pos)
              case Some(t) => Some(Apply(t, List(fun)) setPos tree.pos)
            }
            case _ => None
          }
          case _ => None
        }

        object LiftedView {
          private def analyzer = Select(Select(Select(Select(Ident("eu") setSymbol definitions.getModule("eu"), "stratosphere"), "pact4s"), "common"), "analyzer")

          def unapply(sym: Symbol): Option[(Tree)] = sym match {
            case _ if sym == unanalyzedFieldSelectorCode => Some(Select(Select(analyzer, "FieldSelector"), "unanalyzedFieldSelector"))
            case _ if sym == unanalyzedUDF1Code          => Some(Select(Select(analyzer, "UDF"), "unanalyzedUDF1"))
            case _ if sym == unanalyzedUDF2Code          => Some(Select(Select(analyzer, "UDF"), "unanalyzedUDF2"))
            case _                                       => None
          }
        }
      }

      object UnanalyzedUDF {

        def unapply(tree: Tree): Option[(Tree)] = tree match {
          case Apply(TypeApply(unanalyzed, tparams @ List(_, _)), List(fun)) if unanalyzed.symbol == unanalyzedUDF1 => inferDefaultUDF(tparams map { _.tpe }, fun)
          case Apply(TypeApply(unanalyzed, tparams @ List(_, _, _)), List(fun)) if unanalyzed.symbol == unanalyzedUDF2 => inferDefaultUDF(tparams map { _.tpe }, fun)
          case UnanalyzedFunction(kind, tparams) if (kind != unanalyzedFieldSelector) => Debug.report("Error matching " + kind + " for UDF[" + mkFunctionType(tparams: _*) + "] @ " + tree.id); None
          case _ => None
        }

        private def inferDefaultUDF(tparams: List[Type], fun: Tree): Option[Tree] = {

          val funTpe = mkFunctionType(tparams: _*)
          val tpeudts = tparams.zip(tparams map { tpe => inferImplicitInst(mkUdtOf(unwrapIter(tpe))) })

          val errs = tpeudts filter { !_._2.isDefined } map { _._1 }
          val udts = tpeudts filter { _._2.isDefined } map { _._2.get }

          def analyzer = Select(Select(Select(Select(Ident(definitions.getModule("eu")), "stratosphere"), "pact4s"), "common"), "analyzer")
          def getDefaultUdf(owner: String, kind: String) = Apply(Apply(Select(Select(analyzer, owner), kind), List(fun)), udts)

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
            case _ => {
              val availUdts = localTyper.context.implicitss.flatten.filter(_.name.startsWith("udtInst")).map(m => m.tpe.resultType.typeArgs.head.toString).sorted.mkString(", ")
              val implicitCount = localTyper.context.implicitss.flatten.length
              Debug.report("Could not infer default UDF[" + funTpe + "] @ " + curTree.id + " Missing UDTs: " + errs.mkString(", ") + ". Total Implicits: " + implicitCount + ". Available UDTs: " + availUdts); None
            }
          }
        }
      }

      object UnanalyzedFunction {
        def unapply(tree: Tree): Option[(Symbol, Seq[Type])] = tree match {
          case Apply(TypeApply(unanalyzed, tps @ List(_, _)), List(_)) if unanalyzed.symbol == unanalyzedFieldSelector => Some((unanalyzed.symbol, tps.map(_.tpe)))
          case Apply(TypeApply(unanalyzed, tps @ List(_, _)), List(_)) if unanalyzed.symbol == unanalyzedFieldSelectorCode => Some((unanalyzed.symbol, tps.map(_.tpe)))
          case Apply(TypeApply(unanalyzed, tps @ List(_, _)), List(_)) if unanalyzed.symbol == unanalyzedUDF1 => Some((unanalyzed.symbol, tps.map(_.tpe)))
          case Apply(TypeApply(unanalyzed, tps @ List(_, _)), List(_)) if unanalyzed.symbol == unanalyzedUDF1Code => Some((unanalyzed.symbol, tps.map(_.tpe)))
          case Apply(TypeApply(unanalyzed, tps @ List(_, _, _)), List(_)) if unanalyzed.symbol == unanalyzedUDF2 => Some((unanalyzed.symbol, tps.map(_.tpe)))
          case Apply(TypeApply(unanalyzed, tps @ List(_, _, _)), List(_)) if unanalyzed.symbol == unanalyzedUDF2Code => Some((unanalyzed.symbol, tps.map(_.tpe)))
          case _ => None
        }
      }
    }
  }
}