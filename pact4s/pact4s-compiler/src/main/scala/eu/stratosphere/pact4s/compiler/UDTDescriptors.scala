package eu.stratosphere.pact4s.compiler

trait UDTDescriptors { this: Pact4sGlobal =>

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

  object OpaqueType {

    def unapply(arg: (Type, Type => Tree)): Option[Tree] = {
      val (tpe, infer) = arg

      infer(tpe) match {
        case t if t.symbol == unanalyzedUdt => None
        case ref                            => Some(ref)
      }
    }
  }

  object PrimitiveType {

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

    def unapply(arg: (Type, Type => Tree)): Option[(Literal, Symbol)] = primitives.get(arg._1.typeSymbol)
  }

  object ListType {

    def unapply(arg: (Type, Type => Tree)): Option[(Type, Tree)] = {
      val (tpe, _) = arg

      if (tpe.typeSymbol == definitions.ArrayClass)
        Some((tpe.typeArgs.head, EmptyTree))
      else if (tpe.baseClasses.contains(definitions.getClass("scala.collection.GenTraversableOnce")))
        Some((tpe.typeArgs.head, EmptyTree))
      else
        None
    }
  }

  object CaseClassType {

    def unapply(arg: (Type, Type => Tree)): Boolean = arg._1.typeSymbol.isCaseClass
  }

  object BaseClassType {

    def unapply(arg: (Type, Type => Tree)): Boolean = arg._1.typeSymbol.isClass
  }
}

