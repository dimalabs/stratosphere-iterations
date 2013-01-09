package eu.stratosphere.pact4s.common.analysis.postPass

import eu.stratosphere.pact.compiler.postpass.OptimizerPostPass
import eu.stratosphere.pact.compiler.plan._

trait GlobalSchemaOptimizer extends OptimizerPostPass {

  override def postPass(plan: OptimizedPlan): Unit = {

    val (outputSets, outputPositions) = OutputSets.computeOutputSets(plan)
    val edgeSchemas = EdgeDependencySets.computeEdgeDependencySets(plan, outputSets)

    AmbientFieldDetector.updateAmbientFields(plan, edgeSchemas, outputPositions)

    GlobalSchemaCompactor.compactSchema(plan)

    GlobalSchemaPrinter.printSchema(plan)
  }
}
