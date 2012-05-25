package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.ast.TreeDSL

trait UDTDescriptors extends TreeDSL {

  import global._
  import CODE._

  abstract sealed class UDTDescriptor(val tpe: Type)
  case class PrimitiveDescriptor(myTpe: Type, default: Literal, wrapperClass: Symbol) extends UDTDescriptor(myTpe)
  case class ListDescriptor(myTpe: Type, listType: Type, elem: UDTDescriptor) extends UDTDescriptor(myTpe)
  case class CaseClassDescriptor(myTpe: Type, ctor: Symbol, ctorTpe: Type, getters: Seq[Accessor]) extends UDTDescriptor(myTpe)
  case class Accessor(sym: Symbol, tpe: Type, descr: UDTDescriptor)

  private val udts = mutable.Map[Type, UDTDescriptor]()
  private val genSites = mutable.Map[CompilationUnit, mutable.Map[Tree, Set[UDTDescriptor]]]()

  def getUDTDescriptor(tpe: Type): Either[String, UDTDescriptor] = {
    val normTpe = tpe.map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
    analyzeType(normTpe)
  }

  def getGenSites(unit: CompilationUnit) = genSites.getOrElseUpdate(unit, mutable.Map() withDefaultValue Set())

  private def analyzeType(tpe: Type): Either[String, UDTDescriptor] = {

    val result =
      if (udts.keySet.contains(tpe))
        Right(udts(tpe))
      else if (primitives.keySet.contains(tpe.typeSymbol))
        analyzePrimitive(tpe)
      else if (lists.contains(tpe.typeConstructor.typeSymbol))
        analyzeList(tpe)
      else if (tpe.typeSymbol.isCaseClass)
        analyzeCaseClass(tpe)
      else
        Left("Unsupported type " + tpe)

    for (desc <- result.right) {
      if (!udts.keySet.contains(desc.tpe)) {
        udts += (desc.tpe -> desc)
      }
    }

    result
  }

  private def analyzePrimitive(tpe: Type): Either[String, UDTDescriptor] = {
    val (default, wrapper) = primitives(tpe.typeSymbol)
    Right(PrimitiveDescriptor(tpe, default, wrapper))
  }

  private def analyzeList(tpe: Type): Either[String, UDTDescriptor] = analyzeType(tpe.typeArgs.head) match {
    case Left(err)   => Left(err)
    case Right(desc) => Right(ListDescriptor(tpe, tpe.typeConstructor, desc))
  }

  private def analyzeCaseClass(tpe: Type): Either[String, UDTDescriptor] = {

    val (tParams, tArgs) = tpe.typeConstructor.typeParams.zip(tpe.typeArgs).unzip
    val getters = tpe.typeSymbol.caseFieldAccessors.map(f => (f, f.tpe.instantiateTypeParams(tParams, tArgs).asSeenFrom(tpe.prefix, f.owner.owner)))

    analyzeFields(getters) match {
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
      case Nil           => Left("No constructor found  with signature " + signature + " { " + candidates.map(_._2).mkString(", ") + " }")
    }
  }

  private def analyzeFields(getters: Seq[(Symbol, Type)]): Either[String, Seq[Accessor]] = {
    val fieldDescriptors = getters map { case (fSym, fTpe) => (fSym, fTpe, analyzeType(fTpe.resultType)) }

    fieldDescriptors filter { _._3.isLeft } match {
      case Seq() => Right(fieldDescriptors map { case (fSym, fTpe, Right(desc)) => Accessor(fSym, fTpe, desc) })
      case errs  => Left(errs map { case (fSym, _, Left(err)) => "(" + fSym.name + ": " + err + ")" } mkString (", "))
    }
  }

  private lazy val primitives = Map(
    definitions.BooleanClass -> (FALSE, definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
    definitions.ByteClass -> (LIT(0), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
    definitions.CharClass -> (LIT(0), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
    definitions.DoubleClass -> (LIT(0.0), definitions.getClass("eu.stratosphere.pact.common.type.base.PactDouble")),
    definitions.FloatClass -> (LIT(0.f), definitions.getClass("eu.stratosphere.pact.common.type.base.PactDouble")),
    definitions.IntClass -> (LIT(0), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
    definitions.LongClass -> (LIT(0L), definitions.getClass("eu.stratosphere.pact.common.type.base.PactLong")),
    definitions.ShortClass -> (LIT(0), definitions.getClass("eu.stratosphere.pact.common.type.base.PactInteger")),
    definitions.StringClass -> (NULL, definitions.getClass("eu.stratosphere.pact.common.type.base.PactString"))
  )

  private lazy val lists = Set(definitions.ArrayClass, definitions.SeqClass, definitions.ListClass, definitions.IterableClass, definitions.IteratorClass)
}

