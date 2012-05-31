package eu.stratosphere.pact4s.compiler

import scala.Function.unlift
import scala.collection.mutable

import eu.stratosphere.pact4s.compiler.util.MemoizedPartialFunction
import eu.stratosphere.pact4s.compiler.util.Util

trait UDTMetaDataAnalysis { this: Pact4sGlobal =>

  import global._
  import Severity._

  private val udts = MemoizedPartialFunction[Type, UDTDescriptor]()
  override val getUDT = udts liftWithDefault { tpe: Type => UnsupportedDescriptor(tpe, Seq("Unsupported type")) }

  trait UDTMetaDataAnalyzer { this: UDTAnalyzer =>

    def getUDTDescriptor(tpe: Type): UDTDescriptor = {
      val normTpe = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
      udts(normTpe)
    }

    protected def analyzeUDT(tpe: Type, infer: Type => Tree): UDTDescriptor = {
      val normTpe = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
      analyzeType(normTpe, infer)
    }

    private def analyzeType(tpe: Type, infer: Type => Tree): UDTDescriptor = {

      // TODO (Joe): What if a case class implements GenTraversableOnce?
      // For example - Cons, Nil  <: List[T] <: GenTraversableOnce[T]
      // Or this one - Leaf, Node <: Tree[T] <: GenTraversableOnce[Tree[T]]

      import udts._

      unlessResult (_.isInstanceOf[OpaqueDescriptor]) {
        define(tpe) = {
          verbosely[UDTDescriptor] { d => "Analyzed UDT[" + tpe + "] - " + d.getClass.getName } {
            (tpe, infer) match {
              case OpaqueType(ref)                 => OpaqueDescriptor(tpe, ref)
              case PrimitiveType(default, wrapper) => PrimitiveDescriptor(tpe, default, wrapper)
              case ListType(elemTpe, bf)           => analyzeList(tpe, infer)
              case CaseClassType()                 => analyzeCaseClass(tpe, infer)
              case BaseClassType()                 => analyzeClassHierarchy(tpe, infer)
              case _                               => UnsupportedDescriptor(tpe, Seq("Unsupported type"))
            }
          }
        }
      }
    }

    // TODO (Joe): Handle recursive types

    private def analyzeList(tpe: Type, infer: Type => Tree): UDTDescriptor = analyzeType(tpe.typeArgs.head, infer) match {
      case UnsupportedDescriptor(_, errs) => UnsupportedDescriptor(tpe, errs)
      case desc                           => ListDescriptor(tpe, tpe.typeConstructor, desc)
    }

    private def analyzeClassHierarchy(tpe: Type, infer: Type => Tree): UDTDescriptor = {

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

            analyzeType(dTpe, infer)
          }

          val (subTypes, errors) = Util.partitionByType[UDTDescriptor, UnsupportedDescriptor](descendents)

          errors match {
            case _ :: _ => UnsupportedDescriptor(tpe, errors flatMap { case UnsupportedDescriptor(subType, errs) => errs map { err => "Subtype " + subType + " - " + err } })
            case Nil    => BaseClassDescriptor(tpe, subTypes)
          }
        }
      }
    }

    private def analyzeCaseClass(tpe: Type, infer: Type => Tree): UDTDescriptor = {

      tpe.typeSymbol.superClass.isCaseClass match {

        case true => UnsupportedDescriptor(tpe, Seq("Case-to-case inheritance is not supported."))

        case false => {

          val (tParams, tArgs) = tpe.typeConstructor.typeParams.zip(tpe.typeArgs).unzip
          val getters = tpe.typeSymbol.caseFieldAccessors.map(f => (f, f.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, f.owner.owner)))

          val fields = getters map { case (fSym, fTpe) => FieldAccessor(fSym, fTpe, analyzeType(fTpe.resultType, infer)) }

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
  }
}