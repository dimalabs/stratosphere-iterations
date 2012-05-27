package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global

trait UDTDescriptors {

  val global: Global
  import global._

  lazy val unanalyzedUdt = definitions.getMember(definitions.getModule("eu.stratosphere.pact4s.common.analyzer.UDT"), "unanalyzedUDT")
  lazy val udtClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDT")
  lazy val udtSerializerClass = definitions.getClass("eu.stratosphere.pact4s.common.analyzer.UDTSerializer")
  lazy val pactRecordClass = definitions.getClass("eu.stratosphere.pact.common.type.PactRecord")
  lazy val pactValueClass = definitions.getClass("eu.stratosphere.pact.common.type.Value")
  lazy val pactListClass = definitions.getClass("eu.stratosphere.pact.common.type.base.PactList")

  abstract sealed class UDTDescriptor { val tpe: Type }
  case class OpaqueDescriptor(tpe: Type, ref: Tree) extends UDTDescriptor
  case class PrimitiveDescriptor(tpe: Type, default: Literal, wrapperClass: Symbol) extends UDTDescriptor
  case class ListDescriptor(tpe: Type, listType: Type, elem: UDTDescriptor) extends UDTDescriptor
  case class CaseClassDescriptor(tpe: Type, ctor: Symbol, ctorTpe: Type, getters: Seq[FieldAccessor]) extends UDTDescriptor
  case class FieldAccessor(sym: Symbol, tpe: Type, descr: UDTDescriptor)

  private val udts = mutable.Map[Type, Either[String, UDTDescriptor]]()
  private val genSites = mutable.Map[CompilationUnit, mutable.Map[Tree, Set[UDTDescriptor]]]()

  def analyzeUDT(tpe: Type, infer: Type => Tree): Either[String, UDTDescriptor] = {
    val normTpe = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
    analyzeType(normTpe, infer)
  }

  def getUDTDescriptor(tpe: Type): Either[String, UDTDescriptor] = {
    val normTpe = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
    udts.getOrElse(normTpe, Left("Unsupported type " + tpe))
  }

  def getGenSites(unit: CompilationUnit) = genSites.getOrElseUpdate(unit, mutable.Map() withDefaultValue Set())

  private def analyzeType(tpe: Type, infer: Type => Tree): Either[String, UDTDescriptor] = {

    infer(tpe) match {
      case t if t.symbol == unanalyzedUdt => udts.getOrElseUpdate(tpe, {
        reporter.info(NoPosition, "Analyzing UDT[" + tpe + "]", true)
        tpe match {
          case _ if primitives.keySet.contains(tpe.typeSymbol) => analyzePrimitive(tpe)
          case _ if lists.contains(tpe.typeConstructor.typeSymbol) => analyzeList(tpe, infer)
          case _ if tpe.typeSymbol.isCaseClass => analyzeCaseClass(tpe, infer)
          case _ => Left("Unsupported type " + tpe)
        }
      }
      )
      case ref => Right(OpaqueDescriptor(tpe, ref))
    }
  }

  private def analyzePrimitive(tpe: Type): Either[String, UDTDescriptor] = {
    val (default, wrapper) = primitives(tpe.typeSymbol)
    Right(PrimitiveDescriptor(tpe, default, wrapper))
  }

  private def analyzeList(tpe: Type, infer: Type => Tree): Either[String, UDTDescriptor] = analyzeType(tpe.typeArgs.head, infer) match {
    case Left(err)   => Left(err)
    case Right(desc) => Right(ListDescriptor(tpe, tpe.typeConstructor, desc))
  }

  private def analyzeCaseClass(tpe: Type, infer: Type => Tree): Either[String, UDTDescriptor] = {

    val (tParams, tArgs) = tpe.typeConstructor.typeParams.zip(tpe.typeArgs).unzip
    val getters = tpe.typeSymbol.caseFieldAccessors.map(f => (f, f.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, f.owner.owner)))

    analyzeFields(getters, infer) match {
      case Left(err) => Left(err)
      case Right(fields) => {

        findConstructor(tpe, getters.map(_._2.resultType), tParams, tArgs) match {
          case Left(err)              => Left(err)
          case Right((ctor, ctorTpe)) => Right(CaseClassDescriptor(tpe, ctor, ctorTpe, fields))
        }
      }
    }
  }

  private def findConstructor(tpe: Type, params: Seq[Type], tParams: List[Symbol], tArgs: List[Type]) = {

    val signature = "(" + params.mkString(", ") + ")" + tpe

    val candidates = tpe.members filter { m => m.isPrimaryConstructor } map { ctor => (ctor, ctor.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, ctor.owner.owner)) }
    val ctors = candidates filter { case (ctor, ctorTpe) => ctorTpe.paramTypes.corresponds(params)(_ =:= _) && ctorTpe.resultType =:= tpe }

    ctors match {
      case ctor :: Nil   => Right(ctor)
      case c1 :: c2 :: _ => Left("Multiple constructors found with signature " + signature + " { " + ctors.map(_._2).mkString(", ") + " }")
      case Nil           => Left("No constructor found with signature " + signature + " { " + candidates.map(_._2).mkString(", ") + " }")
    }
  }

  private def analyzeFields(getters: Seq[(Symbol, Type)], infer: Type => Tree): Either[String, Seq[FieldAccessor]] = {
    val fieldDescriptors = getters map { case (fSym, fTpe) => (fSym, fTpe, analyzeType(fTpe.resultType, infer)) }

    fieldDescriptors filter { _._3.isLeft } match {
      case Seq() => Right(fieldDescriptors map { case (fSym, fTpe, Right(desc)) => FieldAccessor(fSym, fTpe, desc) })
      case errs  => Left(errs map { case (fSym, _, Left(err)) => "(" + fSym.name + ": " + err + ")" } mkString (", "))
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

  private lazy val lists = Set(definitions.ArrayClass, definitions.SeqClass, definitions.ListClass, definitions.IterableClass, definitions.IteratorClass)
}

