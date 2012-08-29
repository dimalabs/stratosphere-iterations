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
    lazy val genTraversableOnceClass = definitions.getClass("scala.collection.GenTraversableOnce")
    lazy val canBuildFromClass = definitions.getClass("scala.collection.generic.CanBuildFrom")
    lazy val builderClass = definitions.getClass("scala.collection.mutable.Builder")
    lazy val objectInputStreamClass = definitions.getClass("java.io.ObjectInputStream")

    lazy val unanalyzedUdt = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDT"), "unanalyzedUDT")
    lazy val udtClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDT")
    lazy val udtSerializerClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDTSerializer")
    lazy val pactRecordClass = definitions.getClass("eu.stratosphere.pact.common.type.PactRecord")
    lazy val pactValueClass = definitions.getClass("eu.stratosphere.pact.common.type.Value")
    lazy val pactListBaseClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactList")
    lazy val pactIntegerClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")

    lazy val primitives = Map(
      definitions.BooleanClass -> (Literal(false), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
      definitions.ByteClass -> (Literal(0: Byte), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
      definitions.CharClass -> (Literal(0: Char), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
      definitions.DoubleClass -> (Literal(0: Double), definitions.getClass("eu.stratosphere.pact.common.type.base.PactDouble")),
      definitions.FloatClass -> (Literal(0: Float), definitions.getClass("eu.stratosphere.pact.common.type.base.PactDouble")),
      definitions.IntClass -> (Literal(0: Int), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
      definitions.LongClass -> (Literal(0: Long), definitions.getClass("eu.stratosphere.pact.common.type.base.PactLong")),
      definitions.ShortClass -> (Literal(0: Short), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
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

    def mkIteratorOf(tpe: Type) = appliedType(definitions.IteratorClass.tpe, List(tpe))
    def mkClassOf(tpe: Type) = gen.mkClassOf(tpe)

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