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

trait Definitions { this: Pact4sPlugin =>

  import global._

  object defs {

    lazy val intTpe = definitions.IntClass.tpe
    lazy val intArrayTpe = definitions.arrayType(definitions.IntClass.tpe)
    lazy val iteratorClass = definitions.IteratorClass
    lazy val genTraversableOnceClass = definitions.getClass("scala.collection.GenTraversableOnce")
    lazy val canBuildFromClass = definitions.getClass("scala.collection.generic.CanBuildFrom")
    lazy val builderClass = definitions.getClass("scala.collection.mutable.Builder")
    lazy val objectInputStreamClass = definitions.getClass("java.io.ObjectInputStream")
    lazy val liftMethod = definitions.getMember(definitions.getModule("scala.reflect.Code"), "lift")

    lazy val unanalyzedUdt = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDT"), "unanalyzedUDT")
    lazy val unanalyzedFieldSelector = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.FieldSelector"), "unanalyzedFieldSelector")
    lazy val unanalyzedFieldSelectorCode = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.FieldSelector"), "unanalyzedFieldSelectorCode")
    lazy val unanalyzedUDF1 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDF"), "unanalyzedUDF1")
    lazy val unanalyzedUDF1Code = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDF"), "unanalyzedUDF1Code")
    lazy val unanalyzedUDF2 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDF"), "unanalyzedUDF2")
    lazy val unanalyzedUDF2Code = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDF"), "unanalyzedUDF2Code")

    lazy val analyzedFieldSelectorClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.AnalyzedFieldSelector")
    lazy val analyzedUDF1Class = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF1")
    lazy val analyzedUDF2Class = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF2")

    lazy val defaultUDF1 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF1"), "default")
    lazy val defaultIterTUDF1 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF1"), "defaultIterT")
    lazy val defaultIterRUDF1 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF1"), "defaultIterR")
    lazy val defaultUDF2 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF2"), "default")
    lazy val defaultIterTUDF2 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF2"), "defaultIterT")
    lazy val defaultIterRUDF2 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF2"), "defaultIterR")
    lazy val defaultIterTRUDF2 = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.AnalyzedUDF2"), "defaultIterTR")
    lazy val defaultUDFs = Seq(defaultUDF1, defaultIterTUDF1, defaultIterRUDF1, defaultUDF2, defaultIterTUDF2, defaultIterRUDF2, defaultIterTRUDF2)

    lazy val fieldSelectorCodeClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.FieldSelectorCode")
    lazy val udf1CodeClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDF1Code")
    lazy val udf2CodeClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDF2Code")

    lazy val udtClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDT")
    lazy val udtSerializerClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDTSerializer")
    lazy val pactRecordClass = definitions.getClass("eu.stratosphere.pact.common.type.PactRecord")
    lazy val pactValueClass = definitions.getClass("eu.stratosphere.pact.common.type.Value")
    lazy val pactListBaseClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactList")
    lazy val pactIntegerClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")

    lazy val primitives = Map(
      definitions.BooleanClass -> (Literal(false), definitions.getClass("eu.stratosphere.pact.common.type.base.PactBoolean")),
      definitions.ByteClass -> (Literal(0: Byte), definitions.getClass("eu.stratosphere.pact.common.type.base.PactByte")),
      definitions.CharClass -> (Literal(0: Char), definitions.getClass("eu.stratosphere.pact.common.type.base.PactCharacter")),
      definitions.DoubleClass -> (Literal(0: Double), definitions.getClass("eu.stratosphere.pact.common.type.base.PactDouble")),
      definitions.FloatClass -> (Literal(0: Float), definitions.getClass("eu.stratosphere.pact.common.type.base.PactFloat")),
      definitions.IntClass -> (Literal(0: Int), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
      definitions.LongClass -> (Literal(0: Long), definitions.getClass("eu.stratosphere.pact.common.type.base.PactLong")),
      definitions.ShortClass -> (Literal(0: Short), definitions.getClass("eu.stratosphere.pact.common.type.base.PactShort")),
      definitions.StringClass -> (Literal(null: String), definitions.getClass("eu.stratosphere.pact.common.type.base.PactString"))
    )

    lazy val boxedPrimitives = {

      def getBoxInfo(prim: Symbol, primName: String, boxName: String) = {
        val (default, wrapper) = primitives(prim)
        val box = { t: Tree => Apply(Select(Select(Ident("scala"), "Predef"), primName + "2" + boxName), List(t)) }
        val unbox = { t: Tree => Apply(Select(Select(Ident("scala"), "Predef"), boxName + "2" + primName), List(t)) }
        (default, wrapper, box, unbox)
      }

      Map(
        definitions.BoxedBooleanClass -> getBoxInfo(definitions.BooleanClass, "boolean", "Boolean"),
        definitions.BoxedByteClass -> getBoxInfo(definitions.ByteClass, "byte", "Byte"),
        definitions.BoxedCharacterClass -> getBoxInfo(definitions.CharClass, "char", "Character"),
        definitions.BoxedDoubleClass -> getBoxInfo(definitions.DoubleClass, "double", "Double"),
        definitions.BoxedFloatClass -> getBoxInfo(definitions.FloatClass, "float", "Float"),
        definitions.BoxedIntClass -> getBoxInfo(definitions.IntClass, "int", "Integer"),
        definitions.BoxedLongClass -> getBoxInfo(definitions.LongClass, "long", "Long"),
        definitions.BoxedShortClass -> getBoxInfo(definitions.ShortClass, "short", "Short")
      )
    }

    def mkUdtOf(tpe: Type) = appliedType(udtClass.tpe, List(tpe))
    def mkUdtSerializerOf(tpe: Type) = appliedType(udtSerializerClass.tpe, List(tpe))
    def mkPactListOf(tpe: Type) = appliedType(pactListBaseClass.tpe, List(tpe))

    def mkFieldSelectorCodeOf(tpeT1: Type, tpeR: Type) = appliedType(fieldSelectorCodeClass.tpe, List(definitions.functionType(List(tpeT1), tpeR)))
    def mkUDF1CodeOf(tpeT1: Type, tpeR: Type) = appliedType(udf1CodeClass.tpe, List(definitions.functionType(List(tpeT1), tpeR)))
    def mkUDF2CodeOf(tpeT1: Type, tpeT2: Type, tpeR: Type) = appliedType(udf2CodeClass.tpe, List(definitions.functionType(List(tpeT1, tpeT2), tpeR)))

    def mkCodeView(kind: String, tparams: List[Type]): Type = kind match {
      case "unanalyzedFieldSelector" => { val List(t1, r) = tparams; definitions.functionType(List(mkFunctionType(t1, r)), mkFieldSelectorCodeOf(t1, r)) }
      case "unanalyzedUDF1"          => { val List(t1, r) = tparams; definitions.functionType(List(mkFunctionType(t1, r)), mkUDF1CodeOf(t1, r)) }
      case "unanalyzedUDF2"          => { val List(t1, t2, r) = tparams; mkFunctionType(mkFunctionType(t1, t2, r), mkUDF2CodeOf(t1, t2, r)) }
    }

    def mkIteratorOf(tpe: Type) = appliedType(definitions.IteratorClass.tpe, List(tpe))
    def mkClassOf(tpe: Type) = gen.mkClassOf(tpe)
    def mkFunctionType(tpes: Type*): Type = definitions.functionType(tpes.init.toList, tpes.last)

    def unwrapIter(tpe: Type): Type = isIter(tpe) match {
      case true  => tpe.typeArgs.head
      case false => tpe
    }

    def isIter(tpe: Type): Boolean = tpe.typeSymbol == iteratorClass

    def mkExistentialType(owner: Symbol, tpe: Type, upperBound: Type): Type = {
      val exVar = owner.newAbstractType(newTypeName("_$1")) setInfo TypeBounds.upper(upperBound)
      ExistentialType(List(exVar), appliedType(tpe, List(TypeRef(NoPrefix, exVar, Nil))))
    }

    def mkErasedType(owner: Symbol, tpe: Type): Type = {
      if (tpe.typeConstructor.typeParams.isEmpty) {
        tpe
      } else {
        val exVars = tpe.typeConstructor.typeParams map { _ => TypeRef(NoPrefix, owner.newAbstractType(newTypeName("_")) setInfo TypeBounds.empty, Nil) }
        appliedType(tpe.typeConstructor, exVars)
      }
    }
  }
}