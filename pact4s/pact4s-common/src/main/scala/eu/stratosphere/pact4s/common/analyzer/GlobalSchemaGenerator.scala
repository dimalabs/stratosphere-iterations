package eu.stratosphere.pact4s.common.analyzer

import eu.stratosphere.pact4s.common.contracts._
import eu.stratosphere.pact.common.contract._

trait GlobalSchemaGenerator {

  def initGlobalSchema(outputs: Seq[Pact4sDataSinkContract[_]]) = {

    outputs.foldLeft(0) { (freePos, contract) => globalizeContract(freePos, contract).freePos }
  }

  case class GlobalizeResult(freePos: Int, outputs: Map[Int, Int], forwards: Set[Int])

  def globalizeContract(freePos: Int, contract: Pact4sContract): GlobalizeResult = {
    val result = globalizeContractInner(freePos, contract)
    contract.getParameters().setClassLoader(this.getClass.getClassLoader)
    contract.persistConfiguration()
    result
  }

  def globalizeContractInner(freePos: Int, contract: Pact4sContract): GlobalizeResult = contract match {

    case Pact4sDataSinkContract(input: Pact4sContract, udt, fieldSelector) => {

      var newFreePos = freePos

      if (!fieldSelector.isGlobalized) {

        val GlobalizeResult(freePos1, inputLocations, _) = globalizeContract(freePos, input)
        fieldSelector.globalize(inputLocations)

        newFreePos = freePos1

        printContract(contract)
      }

      GlobalizeResult(newFreePos, Map(), Set())
    }

    case Pact4sDataSourceContract(udt, fieldSelector) => {

      var newFreePos = freePos

      if (!fieldSelector.isGlobalized) {

        val outputLocations = (0 until udt.numFields).map(fieldNum => (fieldNum, fieldNum + freePos)).toMap
        fieldSelector.globalize(outputLocations)

        newFreePos = freePos + udt.numFields

        printContract(contract)
      }

      GlobalizeResult(newFreePos, fieldSelector.getGlobalFields, Set())
    }

    case CoGroup4sContract(left: Pact4sContract, right: Pact4sContract, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {

      var newFreePos = freePos

      if (!udf.isGlobalized) {

        val GlobalizeResult(freePos1, leftInputLocations, leftForwards) = globalizeContract(freePos, left)
        val GlobalizeResult(freePos2, rightInputLocations, rightForwards) = globalizeContract(freePos1, right)

        leftKey.globalize(leftInputLocations)
        rightKey.globalize(rightInputLocations)
        setKeyColumns(contract, leftKey, rightKey)

        newFreePos = udf.globalize(leftInputLocations, rightInputLocations, freePos2)

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

        printContract(contract)
      }

      GlobalizeResult(newFreePos, udf.getOutputFields, udf.getAllForwardedFields.toSet)
    }

    case Cross4sContract(left: Pact4sContract, right: Pact4sContract, leftUdt, rightUdt, udt, udf) => {

      var newFreePos = freePos

      if (!udf.isGlobalized) {

        val GlobalizeResult(freePos1, leftInputLocations, leftForwards) = globalizeContract(freePos, left)
        val GlobalizeResult(freePos2, rightInputLocations, rightForwards) = globalizeContract(freePos1, right)

        newFreePos = udf.globalize(leftInputLocations, rightInputLocations, freePos2)

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

        printContract(contract)
      }

      GlobalizeResult(newFreePos, udf.getOutputFields, udf.getAllForwardedFields.toSet)
    }

    case Join4sContract(left: Pact4sContract, right: Pact4sContract, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {

      var newFreePos = freePos

      if (!udf.isGlobalized) {

        val GlobalizeResult(freePos1, leftInputLocations, leftForwards) = globalizeContract(freePos, left)
        val GlobalizeResult(freePos2, rightInputLocations, rightForwards) = globalizeContract(freePos1, right)

        leftKey.globalize(leftInputLocations)
        rightKey.globalize(rightInputLocations)
        setKeyColumns(contract, leftKey, rightKey)

        newFreePos = udf.globalize(leftInputLocations, rightInputLocations, freePos2)

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

        printContract(contract)
      }

      GlobalizeResult(newFreePos, udf.getOutputFields, udf.getAllForwardedFields.toSet)
    }

    case Map4sContract(input: Pact4sContract, inputUdt, udt, udf) => {

      var newFreePos = freePos

      if (!udf.isGlobalized) {

        val GlobalizeResult(freePos1, inputLocations, forwards) = globalizeContract(freePos, input)

        newFreePos = udf.globalize(inputLocations, freePos1)

        val inputFields = inputLocations.values.toSet union forwards

        for (pos <- inputFields)
          udf.setAmbientFieldBehavior(pos, AmbientFieldBehavior.Forward)

        printContract(contract)
      }

      GlobalizeResult(newFreePos, udf.getOutputFields, udf.getForwardedFields.toSet)
    }

    case Reduce4sContract(input: Pact4sContract, key, inputUdt, udt, cUDF, rUDF) => {

      var newFreePos = freePos

      if (!rUDF.isGlobalized) {

        val GlobalizeResult(freePos1, inputLocations, forwards) = globalizeContract(freePos, input)

        key.globalize(inputLocations)
        setKeyColumns(contract, key)

        cUDF.globalizeInPlace(inputLocations)
        newFreePos = rUDF.globalize(inputLocations, freePos1)

        val inputFields = inputLocations.values.toSet union forwards
        val keyFields = key.getGlobalFields.values.toSet

        for (pos <- inputFields diff keyFields)
          rUDF.setAmbientFieldBehavior(pos, AmbientFieldBehavior.Discard)

        for (pos <- keyFields)
          rUDF.setAmbientFieldBehavior(pos, AmbientFieldBehavior.Forward)

        printContract(contract)
      }

      GlobalizeResult(newFreePos, rUDF.getOutputFields, rUDF.getForwardedFields.toSet)
    }
  }

  private def printContract(contract: Pact4sContract) = {

    contract match {

      case Pact4sDataSinkContract(input: Pact4sContract, udt, fieldSelector) => {
        println(contract.getName() + " (Sink): [" + fieldSelector.getFields.mkString(", ") + "] -> Format")
      }

      case Pact4sDataSourceContract(udt, fieldSelector) => {
        println(contract.getName() + " (Source): Parse -> [" + fieldSelector.getFields.mkString(", ") + "]")
      }

      case CoGroup4sContract(left: Pact4sContract, right: Pact4sContract, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {
        val keyFields = leftKey.getFields.filter(_ >= 0).map("L" + _) ++ rightKey.getFields.filter(_ >= 0).map("R" + _)
        val readFields = udf.getReadFields._1.map("L" + _) ++ udf.getReadFields._2.map("R" + _)
        val forwardedFields = (udf.getForwardedFields._1.map("L" + _) ++ udf.getForwardedFields._2.map("R" + _)).sorted
        val writeFields = udf.getWriteFields.filter(_ >= 0)
        println(contract.getName() + " (CoGroup) {" + keyFields.mkString(", ") + "}: [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
      }

      case Cross4sContract(left: Pact4sContract, right: Pact4sContract, leftUdt, rightUdt, udt, udf) => {
        val readFields = udf.getReadFields._1.map("L" + _) ++ udf.getReadFields._2.map("R" + _)
        val forwardedFields = (udf.getForwardedFields._1.map("L" + _) ++ udf.getForwardedFields._2.map("R" + _)).sorted
        val writeFields = udf.getWriteFields.filter(_ >= 0)
        println(contract.getName() + " (Cross): [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
      }

      case Join4sContract(left: Pact4sContract, right: Pact4sContract, leftKey, rightKey, leftUdt, rightUdt, udt, udf) => {
        val keyFields = leftKey.getFields.filter(_ >= 0).map("L" + _) ++ rightKey.getFields.filter(_ >= 0).map("R" + _)
        val readFields = udf.getReadFields._1.map("L" + _) ++ udf.getReadFields._2.map("R" + _)
        val forwardedFields = (udf.getForwardedFields._1.map("L" + _) ++ udf.getForwardedFields._2.map("R" + _)).sorted
        val writeFields = udf.getWriteFields.filter(_ >= 0)
        println(contract.getName() + " (Join) {" + keyFields.mkString(", ") + "}: [" + readFields.mkString(", ") + "] -> [" + forwardedFields.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
      }

      case Map4sContract(input: Pact4sContract, inputUdt, udt, udf) => {
        val writeFields = udf.getWriteFields.filter(_ >= 0)
        println(contract.getName() + " (Map): [" + udf.getReadFields.mkString(", ") + "] -> [" + udf.getForwardedFields.sorted.mkString(", ") + "] ++ [" + writeFields.mkString(", ") + "]")
      }

      case Reduce4sContract(input: Pact4sContract, key, inputUdt, udt, cUDF, rUDF) => {
        val cWriteFields = cUDF.getWriteFields.filter(_ >= 0)
        val rWriteFields = rUDF.getWriteFields.filter(_ >= 0)
        println(contract.getName() + " (Combine) {" + key.getFields.filter(_ >= 0).mkString(", ") + "}: [" + cUDF.getReadFields.mkString(", ") + "] -> [" + cWriteFields.mkString(", ") + "]")
        println((" " * contract.getName().length) + " (Reduce) {" + key.getFields.filter(_ >= 0).mkString(", ") + "}: [" + rUDF.getReadFields.mkString(", ") + "] -> [" + rUDF.getForwardedFields.sorted.mkString(", ") + "] ++ [" + rWriteFields.mkString(", ") + "]")
      }
    }
  }

  private def setKeyColumns(contract: Pact4sContract, keys: FieldSelector[_]*) = {

    for ((key, inputNum) <- keys.zipWithIndex) {
      val oldKeyColumns = contract.asInstanceOf[AbstractPact[_]].getKeyColumnNumbers(inputNum)
      val newKeyColumns = key.getFields.filter(_ >= 0).toArray
      System.arraycopy(newKeyColumns, 0, oldKeyColumns, 0, newKeyColumns.length)
    }
  }

  /*
  private def ensureUniqueInputs(contract: Pact4sContract): Unit = {

    def createIdentityMapper[Out](input: Pact4sContract, udt: UDT[Out]) = new MapContract(Map4sContract.getStub, input) with Map4sContract[Out, Out] {

      override val inputUDT = udt
      override val outputUDT = udt

      override val mapUDF = new AnalyzedUDF1[Out, Out](udt.numFields, udt.numFields) {
        for (i <- getReadFields)
          markInputFieldCopied(i, i)
      }

      override val userFunction = Left(identity[Out] _)

      this.setName("Identity Mapper")
      this.outDegree = 1
    }

    contract match {

      case c @ Pact4sOneInputContract(input: Pact4sContract) => {

        if (input.outDegree > 1) c.singleInput = createIdentityMapper(input, getOutputUDT(input))
        ensureUniqueInputs(input)
      }

      case c @ Pact4sTwoInputContract(left: Pact4sContract, right: Pact4sContract) => {

        if (left.outDegree > 1) c.leftInput = createIdentityMapper(left, getOutputUDT(left))
        ensureUniqueInputs(left)

        if (right.outDegree > 1) c.rightInput = createIdentityMapper(right, getOutputUDT(right))
        ensureUniqueInputs(right)
      }

      case Pact4sDataSourceContract(_, _) => {}
    }
  }

  /**
   * Computes disjoint write sets for a contract and its inputs.
   *
   * @param contract The contract to globalize
   * @param freePos The next available position in the global schema
   * @return The starting position of this contract's output and the next available position in the global schema
   */
  private def globalize(contract: Pact4sContract, freePos: Int): (Int, Int) = contract match {

    case Pact4sDataSinkContract(input: Pact4sContract, _, fieldSelector) => fieldSelector.getGlobalPosition match {
      case Some(_) => (freePos, freePos)
      case None => {
        val (inputPos, newFreePos) = globalize(input, freePos)
        fieldSelector.globalize(inputPos)
        (-1, newFreePos)
      }
    }

    case Pact4sDataSourceContract(udt, fieldSelector) => fieldSelector.getGlobalPosition match {
      case Some(outputPos) => (outputPos, freePos)
      case None => {
        fieldSelector.globalize(freePos)
        val outputPos = fieldSelector.getGlobalPosition.get
        (outputPos, outputPos + udt.numFields)
      }
    }

    case CoGroup4sContract(left: Pact4sContract, right: Pact4sContract, leftKey, rightKey, _, _, udt, udf) => udf.getGlobalPosition match {
      case Some(outputPos) => (outputPos, freePos)
      case None => {
        val (leftPos, freePos1) = globalize(left, freePos)
        val (rightPos, newFreePos) = globalize(right, freePos1)

        leftKey.globalize(leftPos)
        rightKey.globalize(rightPos)

        udf.globalize(leftPos, rightPos, newFreePos)
        (newFreePos, newFreePos + udt.numFields)
      }
    }

    case Cross4sContract(left: Pact4sContract, right: Pact4sContract, _, _, udt, udf) => udf.getGlobalPosition match {
      case Some(outputPos) => (outputPos, freePos)
      case None => {
        val (leftPos, freePos1) = globalize(left, freePos)
        val (rightPos, newFreePos) = globalize(right, freePos1)

        udf.globalize(leftPos, rightPos, newFreePos)
        (newFreePos, newFreePos + udt.numFields)
      }
    }

    case Join4sContract(left: Pact4sContract, right: Pact4sContract, leftKey, rightKey, _, _, udt, udf) => udf.getGlobalPosition match {
      case Some(outputPos) => (outputPos, freePos)
      case None => {
        val (leftPos, freePos1) = globalize(left, freePos)
        val (rightPos, newFreePos) = globalize(right, freePos1)

        leftKey.globalize(leftPos)
        rightKey.globalize(rightPos)

        udf.globalize(leftPos, rightPos, newFreePos)
        (newFreePos, newFreePos + udt.numFields)
      }
    }

    case Map4sContract(input: Pact4sContract, _, udt, udf) => udf.getGlobalPosition match {
      case Some(outputPos) => (outputPos, freePos)
      case None => {
        val (inputPos, newFreePos) = globalize(input, freePos)
        udf.globalize(inputPos, newFreePos)
        (newFreePos, newFreePos + udt.numFields)
      }
    }

    case Reduce4sContract(input: Pact4sContract, key, _, udt, cUDF, rUDF) => rUDF.getGlobalPosition match {
      case Some(outputPos) => (outputPos, freePos)
      case None => {
        val (inputPos, newFreePos) = globalize(input, freePos)
        key.globalize(inputPos)
        cUDF.globalize(inputPos, inputPos)
        rUDF.globalize(inputPos, newFreePos)
        (newFreePos, newFreePos + udt.numFields)
      }
    }
  }
  
  private def setAmbientFieldBehaviors(contract: Pact4sContract): Array[Int] = contract match {
    
    case Pact4sDataSinkContract(input: Pact4sContract, _, _) => { setAmbientFieldBehaviors(input); Array() }

    case Pact4sDataSourceContract(_, fieldSelector) => fieldSelector.getFields

    case CoGroup4sContract(left: Pact4sContract, right: Pact4sContract, leftKey, rightKey, _, _, _, udf) => udf.getAllForwardedFields match {
      case Array() => {
      }
      case forw => forw ++ udf.getWriteFields
    }

    case Cross4sContract(left: Pact4sContract, right: Pact4sContract, _, _, _, udf) => udf.getAllForwardedFields match {
      case Array() => {
      }
      case forw => forw ++ udf.getWriteFields
    }

    case Join4sContract(left: Pact4sContract, right: Pact4sContract, _, _, _, _, _, udf) => udf.getAllForwardedFields match {
      case Array() => {
        val leftInputs = setAmbientFieldBehaviors(left) toSet
        val rightInputs = setAmbientFieldBehaviors(right) toSet
        
        val disc = leftInputs intersect rightInputs
        val leftForw = leftInputs diff disc
        val rightForw = rightInputs diff disc
        
        disc.foreach { field => 
          udf.setAmbientFieldBehavior(Left(field), AmbientFieldBehavior.Discard) 
          udf.setAmbientFieldBehavior(Right(field), AmbientFieldBehavior.Discard) 
        }
        
        leftForw.foreach { field => udf.setAmbientFieldBehavior(Left(field), AmbientFieldBehavior.Forward) }
        rightForw.foreach { field => udf.setAmbientFieldBehavior(Right(field), AmbientFieldBehavior.Forward) }
        
        (leftForw ++ rightForw).toArray ++ udf.getWriteFields
      }
      case forw => forw ++ udf.getWriteFields
    }

    case Map4sContract(input: Pact4sContract, _, udt, udf) => udf.getForwardedFields match {
      case Array() => {
        val inputs = setAmbientFieldBehaviors(input)
        
        inputs.foreach { udf.setAmbientFieldBehavior(_, AmbientFieldBehavior.Forward) }
        
        inputs ++ udf.getWriteFields
      }
      case forw => forw ++ udf.getWriteFields
    }

    case Reduce4sContract(input: Pact4sContract, key, _, udt, cUDF, rUDF) => rUDF.getForwardedFields match {
      case Array() => {
        val inputs = setAmbientFieldBehaviors(input)
        
        inputs.foreach { field =>
          rUDF.setAmbientFieldBehavior(field, AmbientFieldBehavior.Discard) 
          cUDF.setAmbientFieldBehavior(field, AmbientFieldBehavior.Discard) 
        }
        
        key.getFields.foreach { field =>
          rUDF.setAmbientFieldBehavior(field, AmbientFieldBehavior.Forward) 
          cUDF.setAmbientFieldBehavior(field, AmbientFieldBehavior.Forward) 
        }
        
        key.getFields ++ rUDF.getWriteFields
      }
      case forw => forw ++ rUDF.getWriteFields
    }
  }
  
  private def getOutputUDT(c: Pact4sContract): UDT[_] = c match {

    case Pact4sDataSourceContract(udt, _)            => udt
    case CoGroup4sContract(_, _, _, _, _, _, udt, _) => udt
    case Cross4sContract(_, _, _, _, udt, _)         => udt
    case Join4sContract(_, _, _, _, _, _, udt, _)    => udt
    case Map4sContract(_, _, udt, _)                 => udt
    case Reduce4sContract(_, _, _, udt, _, _)        => udt
  }

  private def getOutputFields(c: Pact4sContract): Array[Int] = (c match {

    case Pact4sDataSourceContract(_, fieldSelector)  => fieldSelector.getFields
    case CoGroup4sContract(_, _, _, _, _, _, _, udf) => udf.getWriteFields
    case Cross4sContract(_, _, _, _, _, udf)         => udf.getWriteFields
    case Join4sContract(_, _, _, _, _, _, _, udf)    => udf.getWriteFields
    case Map4sContract(_, _, _, udf)                 => udf.getWriteFields
    case Reduce4sContract(_, _, _, _, _, rUDF)       => rUDF.getWriteFields
  }) filter { _ >= 0 }
  */
}