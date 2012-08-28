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

package eu.stratosphere.pact4s.compiler.udtgen

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait UDTClassGenerators extends UDTSerializerClassGenerators { this: Pact4sPlugin =>

  import global._
  import defs._

  trait UDTClassGenerator extends UDTSerializerClassGenerator { this: TreeGenerator =>

    protected def mkUdtClass(owner: Symbol, desc: UDTDescriptor): Tree = {

      mkClass(owner, freshTypeName("UDTImpl"), Flags.FINAL, List(definitions.ObjectClass.tpe, mkUdtOf(desc.tpe), definitions.SerializableClass.tpe)) { classSym =>

        val createSerializer = mkMethod(classSym, "createSerializer", Flags.OVERRIDE | Flags.FINAL, List(("indexMap", intArrayTpe)), mkUdtSerializerOf(desc.tpe)) { methodSym =>
          val udtSer = mkUdtSerializerClass(methodSym, desc)
          val inst = New(TypeTree(udtSer.symbol.tpe), List(List()))
          Block(udtSer, inst)
        }

        mkFieldTypes(classSym, desc) :+ createSerializer
      }
    }

    private def mkFieldTypes(udtClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

      val pactValueTpe = mkExistentialType(udtClassSym, definitions.ClassClass.tpe, pactValueClass.tpe)
      val pactListTpe = mkExistentialType(udtClassSym, pactListBaseClass.tpe, pactValueClass.tpe)

      mkValAndGetter(udtClassSym, "fieldTypes", Flags.OVERRIDE | Flags.FINAL, definitions.arrayType(pactValueTpe)) { _ =>

        def getFieldTypes(desc: UDTDescriptor): Seq[Tree] = desc match {
          case PrimitiveDescriptor(_, _, _, wrapper)            => Seq(mkClassOf(wrapper.tpe))
          case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, _) => Seq(mkClassOf(wrapper.tpe))
          case ListDescriptor(_, _, _, _, _, elem)              => Seq(mkClassOf(pactListTpe))
          // Flatten product types
          case CaseClassDescriptor(_, _, _, _, getters)         => getters filterNot { _.isBaseField } flatMap { f => getFieldTypes(f.desc) }
          // Tag and flatten summation types
          // TODO (Joe): Rather than laying subclasses out sequentially, just 
          //             reserve enough fields for the largest subclass.
          //             This is tricky because subclasses can contain opaque
          //             descriptors, so we don't know how many fields we
          //             need until runtime.
          case BaseClassDescriptor(_, _, getters, subTypes)     => (getters flatMap { f => getFieldTypes(f.desc) }) ++ (subTypes flatMap getFieldTypes)
          case OpaqueDescriptor(_, _, ref)                      => Seq(Select(ref, "fieldTypes"))
          // Box inner instances of recursive types
          case RecursiveDescriptor(_, _, _)                     => Seq(mkClassOf(pactRecordClass.tpe))
        }

        val fieldSets = getFieldTypes(desc).foldRight(Seq[Tree]()) { (f, z) =>
          (f, z) match {
            case (_: Select, _)                => f +: z
            case (_, ArrayValue(tpe, fs) :: r) => ArrayValue(tpe, f +: fs) +: r
            case _                             => ArrayValue(TypeTree(pactValueTpe), List(f)) +: z
          }
        }

        fieldSets match {
          case Seq(a) => a
          case as     => Apply(TypeApply(Select(Select(Ident("scala"), "Array"), "concat"), List(TypeTree(pactValueTpe))), as.toList)
        }
      }
    }
  }
}