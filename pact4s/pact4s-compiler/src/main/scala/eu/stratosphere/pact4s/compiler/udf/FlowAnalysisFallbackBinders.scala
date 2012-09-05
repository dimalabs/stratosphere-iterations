package eu.stratosphere.pact4s.compiler.udf

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait FlowAnalysisFallbackBinders { this: Pact4sPlugin =>

  import global._
  import defs._

  trait FlowAnalysisFallbackBinder { this: TypingTransformer with TreeGenerator with Logger =>

    def bindDefaultUDF(tree: Tree): Tree = tree match {
      case UnanalyzedUDF(defaultUDF) => localTyper.typed { defaultUDF }
      case _                         => tree
    }

    private object UnanalyzedUDF {

      private val unanalyzedUDFs = Set(unanalyzedUDF1, unanalyzedUDF2)

      def unapply(tree: Tree): Option[Tree] = tree match {

        case Apply(TypeApply(view, tpeTrees), List(fun)) if unanalyzedUDFs.contains(view.symbol) => {

          val tparams = tpeTrees map { _.tpe }

          inferUdts(tparams) match {

            case Left(errs) => {
              Error.report("Could not infer default UDF[" + mkFunctionType(tparams: _*) + "] @ " + curTree.id + " Missing UDTs: " + errs.mkString(", "))
              None
            }

            case Right(udts) => {
              Debug.report("Inferred default for UDF[" + mkFunctionType(tparams: _*) + "] @ " + curTree.id)

              val (owner, kind) = getDefaultUDFKind(tparams)
              val factory = mkSelect("eu", "stratosphere", "pact4s", "common", "analyzer", owner, kind)
              Some(Apply(Apply(factory, List(fun)), udts))
            }
          }
        }

        case _ => None
      }

      private def inferUdts(tparams: List[Type]): Either[List[Type], List[Tree]] = {

        val (errs, udts) = tparams.foldRight((Nil: List[Type], Nil: List[Tree])) { (tpe, ret) =>
          val udtTpe = mkUdtOf(unwrapIter(tpe))
          inferImplicitInst(udtTpe) match {
            case None      => ret.copy(_1 = tpe :: ret._1)
            case Some(ref) => ret.copy(_2 = ref :: ret._2)
          }
        }

        errs match {
          case Nil => Right(udts)
          case _   => Left(errs)
        }
      }

      // TODO (Joe): Find a better way to figure out what kind of default UDF we want.
      // Going by whether the type signature includes an Iterator prevents supporting
      // Iterator as a List variant for UDT generation.
      private def getDefaultUDFKind(tparams: List[Type]): (String, String) = (tparams: @unchecked) match {
        case List(t1, r) if isIter(t1) => ("AnalyzedUDF1", "defaultIterT")
        case List(t1, r) if isIter(r) => ("AnalyzedUDF1", "defaultIterR")
        case List(t1, r) => ("AnalyzedUDF1", "default")
        case List(t1, t2, r) if isIter(t1) && isIter(t2) && isIter(r) => ("AnalyzedUDF2", "defaultIterTR")
        case List(t1, t2, r) if isIter(t1) && isIter(t2) => ("AnalyzedUDF2", "defaultIterT")
        case List(t1, t2, r) if isIter(r) => ("AnalyzedUDF2", "defaultIterR")
        case List(t1, t2, r) => ("AnalyzedUDF2", "default")
      }
    }
  }
}