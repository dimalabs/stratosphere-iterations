package eu.stratosphere.pact4s.compiler.udf

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait UDFAnalyzers extends Unlifters with FlowAnalysisFallbackBinders { this: Pact4sPlugin =>

  import global._
  import defs._

  trait UDFAnalyzer extends Pact4sComponent {

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with TreeGenerator with Logger with Unlifter with FlowAnalysisFallbackBinder {

      val trans = (unlift _) andThen bindDefaultUDF

      override def apply(tree: Tree) = super.apply { trans(tree) }

    }
  }
}