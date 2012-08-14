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

import scala.collection.mutable
import eu.stratosphere.pact4s.compiler.util.Util

trait UDTAnalysis { this: Pact4sGlobal =>

  import global._

  trait UDTAnalyzer { this: TypingTransformer =>

    private val seen = new MapGate[Type, UDTDescriptor]

    def getUDTDescriptor(tpe: Type, site: Tree): UDTDescriptor = {

      val infer = { tpe: Type => analyzer.inferImplicit(site, appliedType(udtClass.tpe, List(tpe)), true, false, localTyper.context).tree }
      new UDTAnalyzerInstance(infer).analyze(tpe)
    }

    private class UDTAnalyzerInstance(infer: Type => Tree) {

      val cache = new UDTAnalyzerCache()

      def analyze(tpe: Type): UDTDescriptor = {
        val normed = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
        analyzeType(normed)
      }

      private def analyzeType(tpe: Type): UDTDescriptor = {

        // TODO (Joe): What if a case class implements GenTraversableOnce?
        // For example - Cons, Nil  <: List[T] <: GenTraversableOnce[T]
        // Or this one - Leaf, Node <: Tree[T] <: GenTraversableOnce[Tree[T]]

        cache.getOrElseUpdate(tpe) {
          maybeVerbosely(seen(tpe)) { d => "Analyzed UDT[" + tpe + "] - " + d.getClass.getName } {
            tpe match {
              case OpaqueType(ref)                 => OpaqueDescriptor(tpe, ref)
              case PrimitiveType(default, wrapper) => PrimitiveDescriptor(tpe, default, wrapper)
              case ListType(elemTpe, bf, iter)     => analyzeList(tpe, bf, iter)
              case CaseClassType()                 => analyzeCaseClass(tpe)
              case BaseClassType()                 => analyzeClassHierarchy(tpe)
              case _                               => UnsupportedDescriptor(tpe, Seq("Unsupported type"))
            }
          }
        }
      }

      private def analyzeList(tpe: Type, bf: Tree, iter: Tree => Tree): UDTDescriptor = analyzeType(tpe.typeArgs.head) match {
        case UnsupportedDescriptor(_, errs) => UnsupportedDescriptor(tpe, errs)
        case desc                           => ListDescriptor(tpe, tpe.typeConstructor, bf, iter, desc)
      }

      private def analyzeClassHierarchy(tpe: Type): UDTDescriptor = {

        (tpe.typeSymbol.isSealed, tpe.typeSymbol.children) match {

          case (false, _)  => UnsupportedDescriptor(tpe, Seq("Cannot statically determine subtypes for non-sealed base class."))
          case (true, Nil) => UnsupportedDescriptor(tpe, Seq("No subtypes defined for sealed base class."))

          case (true, children) => {

            val descendents = children map { d =>

              val dTpe = verbosely[Type] { dTpe => d.tpe + " <: " + tpe + " instantiated as " + dTpe } {
                val tArgs = (tpe.typeConstructor.typeParams, tpe.typeArgs).zipped.toMap
                val dArgs = d.typeParams map { dp =>
                  val tArg = tArgs.keySet.find { tp => dp == tp.tpe.asSeenFrom(d.tpe, tpe.typeSymbol).typeSymbol }
                  tArg map { tArgs(_) } getOrElse dp.tpe
                }

                appliedType(d.tpe, dArgs)
              }

              analyzeType(dTpe)
            }

            val (subTypes, errors) = Util.partitionByType[UDTDescriptor, UnsupportedDescriptor](descendents)

            errors match {
              case _ :: _ => UnsupportedDescriptor(tpe, errors flatMap { case UnsupportedDescriptor(subType, errs) => errs map { err => "Subtype " + subType + " - " + err } })
              case Nil    => BaseClassDescriptor(tpe, subTypes)
            }
          }
        }
      }

      private def analyzeCaseClass(tpe: Type): UDTDescriptor = {

        tpe.typeSymbol.superClass.isCaseClass match {

          case true => UnsupportedDescriptor(tpe, Seq("Case-to-case inheritance is not supported."))

          case false => {

            val (tParams, tArgs) = tpe.typeConstructor.typeParams.zip(tpe.typeArgs).unzip
            val getters = tpe.typeSymbol.caseFieldAccessors.map(f => (f, f.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, f.owner.owner)))

            val fields = getters map { case (fSym, fTpe) => FieldAccessor(fSym, fTpe, analyzeType(fTpe.resultType)) }

            fields filter { _.descr.isInstanceOf[UnsupportedDescriptor] } match {

              case errs @ _ :: _ => {

                val msgs = errs flatMap { f => (f: @unchecked) match { case FieldAccessor(fSym, _, UnsupportedDescriptor(fTpe, errors)) => errors map { err => "Field " + fSym.name + ": " + fTpe + " - " + err } } }
                UnsupportedDescriptor(tpe, msgs)
              }

              case Nil => {

                findCaseConstructor(tpe, getters.map(_._2.resultType), tParams, tArgs) match {
                  case Left(err)              => UnsupportedDescriptor(tpe, Seq(err))
                  case Right((ctor, ctorTpe)) => CaseClassDescriptor(tpe, ctor, ctorTpe, fields)
                }
              }
            }
          }
        }
      }

      private def findCaseConstructor(tpe: Type, params: Seq[Type], tParams: List[Symbol], tArgs: List[Type]): Either[String, (Symbol, Type)] = {

        val signature = "(" + params.mkString(", ") + ")" + tpe

        val candidates = tpe.members filter { m => m.isPrimaryConstructor } map { ctor => (ctor, ctor.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, ctor.owner.owner)) }
        val ctors = candidates filter { case (ctor, ctorTpe) => ctorTpe.paramTypes.corresponds(params)(_ =:= _) && ctorTpe.resultType =:= tpe }

        ctors match {
          case ctor :: Nil   => Right(ctor)
          case c1 :: c2 :: _ => Left("Multiple constructors found with signature " + signature + " in set { " + ctors.map(_._2).mkString(", ") + " }")
          case Nil           => Left("No constructor found with signature " + signature + " in set { " + candidates.map(_._2).mkString(", ") + " }")
        }
      }

      private object OpaqueType {
        def unapply(tpe: Type): Option[Tree] = infer(tpe) match {
          case EmptyTree                          => None
          case ref if ref.symbol == unanalyzedUdt => None
          case ref                                => Some(ref)
        }
      }

      private object PrimitiveType {

        def unapply(tpe: Type): Option[(Literal, Symbol)] = primitives.get(tpe.typeSymbol)
      }

      private object ListType {

        def unapply(tpe: Type): Option[(Type, Tree, Tree => Tree)] = tpe match {
          case _ if tpe.typeSymbol == definitions.ArrayClass => Some((tpe.typeArgs.head, EmptyTree, arr => Select(Apply(TypeApply(Select(Select(Ident("scala"), "Predef"), "genericArrayOps"), List(TypeTree(tpe.typeArgs.head))), List(arr)), "iterator")))
          case _ if tpe.baseClasses.contains(definitions.getClass("scala.collection.GenTraversableOnce")) => Some((tpe.typeArgs.head, EmptyTree, arr => Select(arr, "toIterator")))
          case _ => None
        }
      }

      private object CaseClassType {

        def unapply(tpe: Type): Boolean = tpe.typeSymbol.isCaseClass
      }

      private object BaseClassType {

        def unapply(tpe: Type): Boolean = tpe.typeSymbol.isClass
      }
    }

    private lazy val primitives = Map(
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

    private class UDTAnalyzerCache {

      private val cache = collection.mutable.Map[Type, UDTDescriptor]()

      def getOrElseUpdate(tpe: Type)(orElse: => UDTDescriptor): UDTDescriptor = cache.getOrElseUpdate(tpe, {
        cache(tpe) = RecursiveDescriptor(tpe, () => cache(tpe))
        val result = orElse
        cache(tpe) = result
        result
      })
    }
  }
}

