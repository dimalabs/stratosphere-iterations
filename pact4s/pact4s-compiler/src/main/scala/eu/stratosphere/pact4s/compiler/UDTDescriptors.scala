package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global

import eu.stratosphere.pact4s.compiler.util.Logger

trait UDTDescriptors extends Logger {

  val global: Global
  import global._

  lazy val unanalyzedUdt = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDT"), "unanalyzedUDT")
  lazy val udtClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDT")
  lazy val udtSerializerClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDTSerializer")
  lazy val pactRecordClass = definitions.getClass("eu.stratosphere.pact.common.type.PactRecord")
  lazy val pactValueClass = definitions.getClass("eu.stratosphere.pact.common.type.Value")
  lazy val pactListClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactList")

  abstract sealed class UDTDescriptor { val tpe: Type }
  case class UnsupportedDescriptor(tpe: Type, errors: Seq[String]) extends UDTDescriptor
  case class OpaqueDescriptor(tpe: Type, ref: Tree) extends UDTDescriptor
  case class PrimitiveDescriptor(tpe: Type, default: Literal, wrapperClass: Symbol) extends UDTDescriptor
  case class ListDescriptor(tpe: Type, listType: Type, elem: UDTDescriptor) extends UDTDescriptor
  case class BaseClassDescriptor(tpe: Type, subTypes: Seq[UDTDescriptor]) extends UDTDescriptor
  case class CaseClassDescriptor(tpe: Type, ctor: Symbol, ctorTpe: Type, getters: Seq[FieldAccessor]) extends UDTDescriptor
  case class FieldAccessor(sym: Symbol, tpe: Type, descr: UDTDescriptor)

  private val udts = mutable.Map[Type, UDTDescriptor]()
  private val genSites = mutable.Map[CompilationUnit, mutable.Map[Tree, Set[UDTDescriptor]]]()

  def analyzeUDT(tpe: Type, infer: Type => Tree): UDTDescriptor = {
    val normTpe = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
    analyzeType(normTpe, infer)
  }

  def getUDTDescriptor(tpe: Type): UDTDescriptor = {
    val normTpe = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
    udts.getOrElse(normTpe, UnsupportedDescriptor(tpe, Seq("Unsupported type")))
  }

  def getGenSites(unit: CompilationUnit) = genSites.getOrElseUpdate(unit, mutable.Map() withDefaultValue Set())

  var curPos: Position = _
  override def messageTag = "Ana"
  override def currentPosition = curPos

  private def analyzeType(tpe: Type, infer: Type => Tree): UDTDescriptor = {

    infer(tpe) match {
      case t if t.symbol == unanalyzedUdt =>
        udts.getOrElseUpdate(tpe, {
          verbosely[UDTDescriptor] { _ => "Analyzing UDT[" + tpe + "]" } {
            // TODO (Joe): What if a case class implements GenTraversableOnce?
            // For example - Cons, Nil  <: List[T] <: GenTraversableOnce[T]
            // Or this one - Leaf, Node <: Tree[T] <: GenTraversableOnce[Tree[T]]
            tpe match {
              case _ if primitives.keySet.contains(tpe.typeSymbol) => analyzePrimitive(tpe)
              case _ if lists.intersect(tpe.baseClasses.toSet).nonEmpty => analyzeList(tpe, infer)
              case _ if tpe.typeSymbol.isCaseClass => analyzeCaseClass(tpe, infer)
              case _ if tpe.typeSymbol.isClass => analyzeClassHierarchy(tpe, infer)
              case _ => UnsupportedDescriptor(tpe, Seq("Unsupported type"))
            }
          }
        })

      case ref => OpaqueDescriptor(tpe, ref)
    }
  }

  private def analyzePrimitive(tpe: Type): UDTDescriptor = {
    val (default, wrapper) = primitives(tpe.typeSymbol)
    PrimitiveDescriptor(tpe, default, wrapper)
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

        val (subTypes, errors) = partitionByType[UDTDescriptor, UnsupportedDescriptor](descendents)

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

  private def partitionByType[T, S <: T: Manifest](items: List[T]): (List[T], List[S]) = {
    val sClass = implicitly[Manifest[S]].erasure
    val (sAsT, t) = items.partition { item => sClass.isAssignableFrom(item.getClass) }
    (t, sAsT map { _.asInstanceOf[S] })
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

  private lazy val lists = Set(definitions.ArrayClass, definitions.getClass("scala.collection.GenTraversableOnce"))
}

