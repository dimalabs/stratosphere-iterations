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

import scala.util.DynamicVariable

import eu.stratosphere.pact4s.compiler.util.Counter

trait UDTAnalyzers { this: Pact4sPlugin =>

  import global._
  import defs._

  trait UDTAnalyzer { this: TypingTransformer with LoggingTransformer =>

    private val seen = new MapGate[Type, UDTDescriptor]

    def getUDTDescriptor(tpe: Type, site: Tree): UDTDescriptor = {

      val currentThis = ThisType(localTyper.context.enclClass.owner)
      val norm = { tpe: Type => currentThis.baseClasses.foldLeft(tpe map { _.dealias }) { (tpe, base) => tpe.substThis(base, currentThis) } }
      val infer = { tpe: Type => analyzer.inferImplicit(site, tpe, true, false, localTyper.context).tree }

      new UDTAnalyzerInstance(norm, infer).analyze(tpe)
    }

    private class UDTAnalyzerInstance(normTpe: Type => Type, infer: Type => Tree) {

      private val cache = new UDTAnalyzerCache()

      def analyze(tpe: Type): UDTDescriptor = {

        val normed = normTpe(tpe)

        // TODO (Joe): Fix issues with Nothing type

        cache.getOrElseUpdate(normed) { id =>
          maybeVerbosely(seen(normed)) { d => "Analyzed UDT[" + tpe + " ~> " + normed + "] - " + d.getClass.getName } {
            normed match {
              case OpaqueType(ref) => OpaqueDescriptor(id, normed, ref)
              case PrimitiveType(default, wrapper) => PrimitiveDescriptor(id, normed, default, wrapper)
              case BoxedPrimitiveType(default, wrapper, box, unbox) => BoxedPrimitiveDescriptor(id, normed, default, wrapper, box, unbox)
              case ListType(elemTpe, bf, iter) => analyzeList(id, normed, bf, iter)
              case CaseClassType() => analyzeCaseClass(id, normed)
              case BaseClassType() => analyzeClassHierarchy(id, normed)
              case _ => UnsupportedDescriptor(id, normed, Seq("Unsupported type " + normed))
            }
          }
        }
      }

      private def analyzeList(id: Int, tpe: Type, cbf: () => Tree, iter: Tree => Tree): UDTDescriptor = analyze(tpe.typeArgs.head) match {
        case UnsupportedDescriptor(_, _, errs) => UnsupportedDescriptor(id, tpe, errs)
        case desc                              => ListDescriptor(id, tpe, tpe.typeConstructor, cbf, iter, desc)
      }

      private def analyzeClassHierarchy(id: Int, tpe: Type): UDTDescriptor = {

        val tagField = {
          val (intTpe, intDefault, intWrapper) = PrimitiveType.intPrimitive
          FieldAccessor(NoSymbol, NullaryMethodType(intTpe), true, PrimitiveDescriptor(cache.newId, intTpe, intDefault, intWrapper))
        }

        val subTypes = tpe.typeSymbol.children flatMap { d =>

          val dTpe = verbosely[Type] { dTpe => d.tpe + " <: " + tpe + " instantiated as " + dTpe + " (" + (if (dTpe <:< tpe) "Valid" else "Invalid") + " subtype)" } {
            val tArgs = (tpe.typeConstructor.typeParams, tpe.typeArgs).zipped.toMap
            val dArgs = d.typeParams map { dp =>
              val tArg = tArgs.keySet.find { tp => dp == tp.tpe.asSeenFrom(d.tpe, tpe.typeSymbol).typeSymbol }
              tArg map { tArgs(_) } getOrElse dp.tpe
            }

            normTpe(appliedType(d.tpe, dArgs))
          }

          if (dTpe <:< tpe)
            Some(analyze(dTpe))
          else
            None
        }

        val errors = subTypes flatMap { _.findByType[UnsupportedDescriptor] }

        errors match {
          case _ :: _                  => UnsupportedDescriptor(id, tpe, errors flatMap { case UnsupportedDescriptor(_, subType, errs) => errs map { err => "Subtype " + subType + " - " + err } })
          case Nil if subTypes.isEmpty => UnsupportedDescriptor(id, tpe, Seq("No instantiable subtypes found for base class"))
          case Nil => {

            val (tParams, tArgs) = tpe.typeConstructor.typeParams.zip(tpe.typeArgs).unzip
            val baseMembers = tpe.members.reverse filter { f => f.isGetter } map { f => (f, normTpe(f.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, f.owner.owner))) }

            val subMembers = subTypes map {
              case BaseClassDescriptor(_, _, getters, _)    => getters
              case CaseClassDescriptor(_, _, _, _, getters) => getters
              case _                                        => Seq()
            }

            val baseFields = baseMembers flatMap {
              case (bSym, bTpe) => {
                val accessors = subMembers map { _ find { sf => sf.sym.name == bSym.name && sf.tpe.resultType <:< bTpe.resultType } }
                accessors.forall { _.isDefined } match {
                  case true  => Some(FieldAccessor(bSym, bTpe, true, analyze(bTpe.resultType)))
                  case false => None
                }
              }
            }

            def wireBaseFields(desc: UDTDescriptor): UDTDescriptor = {

              def updateField(field: FieldAccessor) = {
                baseFields find { bf => bf.sym.name == field.sym.name } match {
                  case Some(FieldAccessor(_, _, _, desc)) => field.copy(isBaseField = true, desc = desc)
                  case None                               => field
                }
              }

              desc match {
                case desc @ BaseClassDescriptor(_, _, getters, subTypes) => desc.copy(getters = getters map updateField, subTypes = subTypes map wireBaseFields)
                case desc @ CaseClassDescriptor(_, _, _, _, getters) => desc.copy(getters = getters map updateField)
                case _ => desc
              }
            }

            Debug.report("BaseClass " + tpe + " has shared fields: " + (baseFields.map { m => m.sym.name + ": " + m.tpe }))
            BaseClassDescriptor(id, tpe, tagField +: baseFields, subTypes map wireBaseFields)
          }
        }

      }

      private def analyzeCaseClass(id: Int, tpe: Type): UDTDescriptor = {

        tpe.typeSymbol.superClass.isCaseClass match {

          case true => UnsupportedDescriptor(id, tpe, Seq("Case-to-case inheritance is not supported."))

          case false => {

            val (tParams, tArgs) = tpe.typeConstructor.typeParams.zip(tpe.typeArgs).unzip
            val getters = tpe.typeSymbol.caseFieldAccessors map { f => (f, f.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, f.owner.owner)) }

            val fields = getters map { case (fSym, fTpe) => FieldAccessor(fSym, fTpe, false, analyze(fTpe.resultType)) }

            fields filter { _.desc.isInstanceOf[UnsupportedDescriptor] } match {

              case errs @ _ :: _ => {

                val msgs = errs flatMap { f => (f: @unchecked) match { case FieldAccessor(fSym, _, _, UnsupportedDescriptor(_, fTpe, errors)) => errors map { err => "Field " + fSym.name + ": " + fTpe + " - " + err } } }
                UnsupportedDescriptor(id, tpe, msgs)
              }

              case Nil => {

                findCaseConstructor(tpe, getters.map(_._2.resultType), tParams, tArgs) match {
                  case Left(err)              => UnsupportedDescriptor(id, tpe, Seq(err))
                  case Right((ctor, ctorTpe)) => CaseClassDescriptor(id, tpe, ctor, ctorTpe, fields)
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

        private val treeCopy = new Transformer {
          override val treeCopy = new StrictTreeCopier
        }

        def unapply(tpe: Type): Option[() => Tree] = infer(mkUdtOf(tpe)) match {
          case EmptyTree                          => None
          case ref if ref.symbol == unanalyzedUdt => None
          case ref                                => Some(() => treeCopy.transform(ref))
        }
      }

      private object PrimitiveType {

        def intPrimitive: (Type, Literal, Symbol) = {
          val (d, w) = primitives(definitions.IntClass)
          (definitions.IntClass.tpe, d, w)
        }

        def unapply(tpe: Type): Option[(Literal, Symbol)] = primitives.get(tpe.typeSymbol)
      }

      private object BoxedPrimitiveType {

        def unapply(tpe: Type): Option[(Literal, Symbol, Tree => Tree, Tree => Tree)] = boxedPrimitives.get(tpe.typeSymbol)
      }

      private object ListType {

        private val treeCopy = new Transformer {
          override val treeCopy = new StrictTreeCopier
        }

        def unapply(tpe: Type): Option[(Type, () => Tree, Tree => Tree)] = tpe match {

          case ArrayType(elemTpe) => {
            val iter = { source: Tree => Select(Apply(TypeApply(Select(Select(Ident("scala"), "Predef"), "genericArrayOps"), List(TypeTree(elemTpe))), List(source)), "iterator") }
            withCanBuildFrom(tpe, elemTpe, iter)
          }

          case TraversableType(elemTpe) => {
            val iter = { source: Tree => Select(source, "toIterator") }
            withCanBuildFrom(tpe, elemTpe, iter)
          }

          case _ => None
        }

        private def withCanBuildFrom(tpe: Type, elemTpe: Type, iter: Tree => Tree): Option[(Type, () => Tree, Tree => Tree)] = {
          val cbfTpe = appliedType(canBuildFromClass.tpe, List(tpe, elemTpe, tpe))
          infer(cbfTpe) match {
            case EmptyTree => None
            case cbf       => Some((elemTpe, () => treeCopy.transform(cbf), iter))
          }
        }

        private object ArrayType {
          def unapply(tpe: Type): Option[Type] = tpe match {
            case _ if tpe.typeSymbol == definitions.ArrayClass => Some(tpe.typeArgs.head)
            case _ => None
          }
        }

        private object TraversableType {
          def unapply(tpe: Type): Option[Type] = tpe match {
            case _ if tpe.baseClasses.contains(genTraversableOnceClass) => {
              val abstrElemTpe = genTraversableOnceClass.typeConstructor.typeParams.head.tpe
              val elemTpe = abstrElemTpe.asSeenFrom(tpe, genTraversableOnceClass)
              Some(elemTpe)
            }
            case _ => None
          }
        }
      }

      private object CaseClassType {

        def unapply(tpe: Type): Boolean = tpe.typeSymbol.isCaseClass
      }

      private object BaseClassType {

        def unapply(tpe: Type): Boolean = tpe.typeSymbol.isAbstractClass && tpe.typeSymbol.isSealed
      }

      private class UDTAnalyzerCache {

        private val caches = new DynamicVariable[Map[Type, RecursiveDescriptor]](Map())
        private val idGen = new Counter()

        def newId = idGen.next

        def getOrElseUpdate(tpe: Type)(orElse: Int => UDTDescriptor): UDTDescriptor = {

          val id = idGen.next
          val cache = caches.value

          cache.get(tpe) map { _.copy(id = id) } getOrElse {
            val ref = RecursiveDescriptor(id, tpe, id)
            caches.withValue(cache + (tpe -> ref)) {
              orElse(id)
            }
          }
        }
      }
    }

  }
}

