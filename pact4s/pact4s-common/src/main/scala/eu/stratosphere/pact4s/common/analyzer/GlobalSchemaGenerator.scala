package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact.common.contract._

trait GlobalSchemaGenerator {

  def initGlobalSchema(outputs: Seq[Pact4sDataSinkContract[_]]) = {

    val proxies: Map[Contract, Pact4sContract] = Map() withDefault { _.asInstanceOf[Pact4sContract] }
    implicit val printer = new SchemaPrinter
    outputs.foldLeft(0) { (freePos, contract) => globalizeContract(freePos, contract, proxies, None).freePos }
  }

  case class GlobalizeResult(freePos: Int, outputs: Map[Int, Int], forwards: Set[Int])

  /**
   * Computes disjoint write sets for a contract and its inputs.
   *
   * @param freePos The next available position in the global schema
   * @param contract The contract to globalize
   * @param proxies Provides contracts for placeholders
   * @param predeterminedOutputLocations Specifies required positions for the contract's output fields, or None to allocate new positions
   * @return A GlobalizeResult containing the next available position in the global schema and the locations of the contract's output fields
   */
  def globalizeContract(freePos: Int, contract: Contract, proxies: Map[Contract, Pact4sContract], predeterminedOutputLocations: Option[Map[Int, Int]])(implicit printer: SchemaPrinter): GlobalizeResult = {

    val result = proxies(contract) match {

      case contract4s @ Pact4sDataSinkContract(input, udt, fieldSelector) => {

        var newFreePos = freePos

        if (!fieldSelector.isGlobalized) {

          val GlobalizeResult(freePos1, inputLocations, _) = globalizeContract(freePos, input, proxies, None)
          fieldSelector.globalize(inputLocations)

          newFreePos = freePos1

          prepareContract(contract4s)
        }

        GlobalizeResult(newFreePos, Map(), Set())
      }

      case contract4s @ Pact4sDataSourceContract(udt, fieldSelector) => {

        var newFreePos = freePos

        if (!fieldSelector.isGlobalized) {

          var outputLocations: Map[Int, Int] = null

          if (predeterminedOutputLocations.isDefined) {
            outputLocations = predeterminedOutputLocations.get
          } else {
            newFreePos = freePos + udt.numFields
            outputLocations = (0 until udt.numFields).map(fieldNum => (fieldNum, fieldNum + freePos)).toMap
          }

          fieldSelector.globalize(outputLocations)

          prepareContract(contract4s)
        }

        GlobalizeResult(newFreePos, fieldSelector.getGlobalFields, Set())
      }

      case contract4s @ Iterate4sContract(s0, step, term, placeholder) => {

        val newProxies = proxies + (placeholder -> proxies.getOrElse(s0, s0.asInstanceOf[Pact4sContract]))

        val GlobalizeResult(freePos1, outputs, _) = globalizeContract(freePos, s0, proxies, predeterminedOutputLocations)
        val GlobalizeResult(freePos2, _, forwards) = globalizeContract(freePos1, step, newProxies, Some(outputs))

        var newFreePos = freePos2

        if (term != null)
          newFreePos = globalizeContract(freePos2, term, newProxies, None).freePos

        GlobalizeResult(newFreePos, outputs, forwards)
      }

      case contract4s @ WorksetIterate4sContract(s0, ws0, key, deltaS, newWS, placeholderS, placeholderWS) => {

        val newProxies = proxies + (placeholderS -> proxies.getOrElse(s0, s0.asInstanceOf[Pact4sContract])) + (placeholderWS -> proxies.getOrElse(ws0, ws0.asInstanceOf[Pact4sContract]))

        val GlobalizeResult(freePos1, sOutputs, _) = globalizeContract(freePos, s0, proxies, predeterminedOutputLocations)
        val GlobalizeResult(freePos2, wsOutputs, _) = globalizeContract(freePos1, ws0, proxies, None)
        val GlobalizeResult(freePos3, _, forwards) = globalizeContract(freePos2, deltaS, newProxies, Some(sOutputs))
        val GlobalizeResult(freePos4, _, _) = globalizeContract(freePos3, newWS, newProxies, Some(wsOutputs))

        GlobalizeResult(freePos4, sOutputs, forwards)
      }

      case contract4s @ CoGroup4sContract(left, right, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {

        var newFreePos = freePos

        if (!udf.isGlobalized) {

          val GlobalizeResult(freePos1, leftInputLocations, leftForwards) = globalizeContract(freePos, left, proxies, None)
          val GlobalizeResult(freePos2, rightInputLocations, rightForwards) = globalizeContract(freePos1, right, proxies, None)

          leftKey.globalize(leftInputLocations)
          rightKey.globalize(rightInputLocations)
          setKeyColumns(contract4s, leftKey, rightKey)

          newFreePos = udf.globalize(leftInputLocations, rightInputLocations, freePos2, predeterminedOutputLocations)

          val leftFields = leftInputLocations.values.toSet union leftForwards
          val rightFields = rightInputLocations.values.toSet union rightForwards
          val leftKeyFields = leftKey.getGlobalFields.values.toSet
          val rightKeyFields = rightKey.getGlobalFields.values.toSet

          for (pos <- leftFields diff leftKeyFields)
            udf.setAmbientFieldBehavior(Left(pos), AmbientFieldBehavior.Discard)

          for (pos <- rightFields diff rightKeyFields)
            udf.setAmbientFieldBehavior(Right(pos), AmbientFieldBehavior.Discard)

          for (pos <- leftKeyFields)
            udf.setAmbientFieldBehavior(Left(pos), AmbientFieldBehavior.Forward)

          for (pos <- rightKeyFields)
            udf.setAmbientFieldBehavior(Right(pos), AmbientFieldBehavior.Forward)

          prepareContract(contract4s)
        }

        GlobalizeResult(newFreePos, udf.getOutputFields, udf.getAllForwardedFields.toSet)
      }

      case contract4s @ Cross4sContract(left, right, leftUdt, rightUdt, udt, udf) => {

        var newFreePos = freePos

        if (!udf.isGlobalized) {

          val GlobalizeResult(freePos1, leftInputLocations, leftForwards) = globalizeContract(freePos, left, proxies, None)
          val GlobalizeResult(freePos2, rightInputLocations, rightForwards) = globalizeContract(freePos1, right, proxies, None)

          newFreePos = udf.globalize(leftInputLocations, rightInputLocations, freePos2, predeterminedOutputLocations)

          val leftFields = leftInputLocations.values.toSet union leftForwards
          val rightFields = rightInputLocations.values.toSet union rightForwards
          val conflicts = leftFields intersect rightFields

          for (pos <- conflicts) {
            udf.setAmbientFieldBehavior(Left(pos), AmbientFieldBehavior.Discard)
            udf.setAmbientFieldBehavior(Right(pos), AmbientFieldBehavior.Discard)
          }

          for (pos <- leftFields diff conflicts)
            udf.setAmbientFieldBehavior(Left(pos), AmbientFieldBehavior.Forward)

          for (pos <- rightFields diff conflicts)
            udf.setAmbientFieldBehavior(Right(pos), AmbientFieldBehavior.Forward)

          prepareContract(contract4s)
        }

        GlobalizeResult(newFreePos, udf.getOutputFields, udf.getAllForwardedFields.toSet)
      }

      case contract4s @ Join4sContract(left, right, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {

        var newFreePos = freePos

        if (!udf.isGlobalized) {

          val GlobalizeResult(freePos1, leftInputLocations, leftForwards) = globalizeContract(freePos, left, proxies, None)
          val GlobalizeResult(freePos2, rightInputLocations, rightForwards) = globalizeContract(freePos1, right, proxies, None)

          leftKey.globalize(leftInputLocations)
          rightKey.globalize(rightInputLocations)
          setKeyColumns(contract4s, leftKey, rightKey)

          newFreePos = udf.globalize(leftInputLocations, rightInputLocations, freePos2, predeterminedOutputLocations)

          val leftKeyFields = leftKey.getGlobalFields.values.toSet
          val rightKeyFields = rightKey.getGlobalFields.values.toSet

          val leftFields = leftInputLocations.values.toSet union leftForwards
          val rightFields = rightInputLocations.values.toSet union rightForwards
          val conflicts = leftFields intersect rightFields

          for (pos <- conflicts) {
            udf.setAmbientFieldBehavior(Left(pos), AmbientFieldBehavior.Discard)
            udf.setAmbientFieldBehavior(Right(pos), AmbientFieldBehavior.Discard)
          }

          for (pos <- leftFields diff conflicts union leftKeyFields)
            udf.setAmbientFieldBehavior(Left(pos), AmbientFieldBehavior.Forward)

          for (pos <- rightFields diff conflicts union rightKeyFields)
            udf.setAmbientFieldBehavior(Right(pos), AmbientFieldBehavior.Forward)

          prepareContract(contract4s)
        }

        GlobalizeResult(newFreePos, udf.getOutputFields, udf.getAllForwardedFields.toSet)
      }

      case contract4s @ Map4sContract(input, inputUdt, udt, udf) => {

        var newFreePos = freePos

        if (!udf.isGlobalized) {

          val GlobalizeResult(freePos1, inputLocations, forwards) = globalizeContract(freePos, input, proxies, None)

          newFreePos = udf.globalize(inputLocations, freePos1, predeterminedOutputLocations)

          val inputFields = inputLocations.values.toSet union forwards

          for (pos <- inputFields)
            udf.setAmbientFieldBehavior(pos, AmbientFieldBehavior.Forward)

          prepareContract(contract4s)
        }

        GlobalizeResult(newFreePos, udf.getOutputFields, udf.getForwardedFields.toSet)
      }

      case contract4s @ Reduce4sContract(input, key, inputUdt, udt, cUDF, rUDF) => {

        var newFreePos = freePos

        if (!rUDF.isGlobalized) {

          val GlobalizeResult(freePos1, inputLocations, forwards) = globalizeContract(freePos, input, proxies, None)

          key.globalize(inputLocations)
          setKeyColumns(contract4s, key)

          cUDF.globalize(inputLocations, freePos1, Some(inputLocations))
          newFreePos = rUDF.globalize(inputLocations, freePos1, predeterminedOutputLocations)

          val inputFields = inputLocations.values.toSet union forwards
          val keyFields = key.getGlobalFields.values.toSet

          for (pos <- inputFields diff keyFields)
            rUDF.setAmbientFieldBehavior(pos, AmbientFieldBehavior.Discard)

          for (pos <- keyFields)
            rUDF.setAmbientFieldBehavior(pos, AmbientFieldBehavior.Forward)

          prepareContract(contract4s)
        }

        GlobalizeResult(newFreePos, rUDF.getOutputFields, rUDF.getForwardedFields.toSet)
      }
    }

    printer.printSchema(contract, proxies)
    result
  }

  private def setKeyColumns(contract: Pact4sContract, keys: FieldSelector[_]*) = {

    for ((key, inputNum) <- keys.zipWithIndex) {
      val oldKeyColumns = contract.asInstanceOf[AbstractPact[_]].getKeyColumnNumbers(inputNum)
      val newKeyColumns = key.getFields.filter(_ >= 0).toArray
      System.arraycopy(newKeyColumns, 0, oldKeyColumns, 0, newKeyColumns.length)
    }
  }

  private def prepareContract(contract: Pact4sContract) = {

    contract.getParameters().setClassLoader(this.getClass.getClassLoader)
    contract.persistConfiguration()
  }

  private class SchemaPrinter {

    private val printed = collection.mutable.Set[Contract]()

    def printSchema(contract: Contract, proxies: Map[Contract, Pact4sContract]) = {

      if (printed.add(contract)) {

        contract match {

          case Pact4sDataSinkContract(input, udt, fieldSelector) => {
            println(contract.getName() + " (Sink): [" + fieldSelector.getFields.mkString(", ") + "] -> Format")
          }

          case Pact4sDataSourceContract(udt, fieldSelector) => {
            println(contract.getName() + " (Source): Parse -> [" + fieldSelector.getFields.mkString(", ") + "]")
          }

          case Iterate4sContract(s0, step, term, placeholder) => {
            println(contract.getName() + " (Iterate)")
          }

          case WorksetIterate4sContract(s0, ws0, key, deltaS, newWS, placeholderS, placeholderWS) => {
            println(contract.getName() + " (WorksetIterate)")
          }

          case CoGroup4sContract(left, right, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {
            val keyFields = leftKey.getFields.filter(_ >= 0).map("L" + _) ++ rightKey.getFields.filter(_ >= 0).map("R" + _)
            val readFields = udf.getReadFields._1.map("L" + _) ++ udf.getReadFields._2.map("R" + _)
            val forwardedFields = (udf.getForwardedFields._1.map("L" + _) ++ udf.getForwardedFields._2.map("R" + _)).sorted
            val writeFields = udf.getWriteFields.filter(_ >= 0)
            println(contract.getName() + " (CoGroup) {" + keyFields.mkString(", ") + "}: [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
          }

          case Cross4sContract(left, right, leftUdt, rightUdt, udt, udf) => {
            val readFields = udf.getReadFields._1.map("L" + _) ++ udf.getReadFields._2.map("R" + _)
            val forwardedFields = (udf.getForwardedFields._1.map("L" + _) ++ udf.getForwardedFields._2.map("R" + _)).sorted
            val writeFields = udf.getWriteFields.filter(_ >= 0)
            println(contract.getName() + " (Cross): [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
          }

          case Join4sContract(left, right, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {
            val keyFields = leftKey.getFields.filter(_ >= 0).map("L" + _) ++ rightKey.getFields.filter(_ >= 0).map("R" + _)
            val readFields = udf.getReadFields._1.map("L" + _) ++ udf.getReadFields._2.map("R" + _)
            val forwardedFields = (udf.getForwardedFields._1.map("L" + _) ++ udf.getForwardedFields._2.map("R" + _)).sorted
            val writeFields = udf.getWriteFields.filter(_ >= 0)
            println(contract.getName() + " (Join) {" + keyFields.mkString(", ") + "}: [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
          }

          case Map4sContract(input, inputUdt, udt, udf) => {
            val writeFields = udf.getWriteFields.filter(_ >= 0)
            println(contract.getName() + " (Map): [" + udf.getReadFields.mkString(", ") + "] -> [" + udf.getForwardedFields.sorted.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
          }

          case Reduce4sContract(input, key, inputUdt, udt, cUDF, rUDF) => {
            val cWriteFields = cUDF.getWriteFields.filter(_ >= 0)
            val rWriteFields = rUDF.getWriteFields.filter(_ >= 0)
            println(contract.getName() + " (Combine) {" + key.getFields.filter(_ >= 0).mkString(", ") + "}: [" + cUDF.getReadFields.mkString(", ") + "] -> [" + cWriteFields.mkString(", ") + "]")
            println((" " * contract.getName().length) + " (Reduce) {" + key.getFields.filter(_ >= 0).mkString(", ") + "}: [" + rUDF.getReadFields.mkString(", ") + "] -> [" + rUDF.getForwardedFields.sorted.mkString(", ") + "] ++ [" + rWriteFields.mkString(", ") + "]")
          }

          case proxy => {
            println(proxies(proxy).getName() + " (Proxy)")
          }
        }
      }
    }
  }
}