package eu.stratosphere.pact4s.compiler.selector

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait SelectionValidators { this: Pact4sPlugin =>

  import global._
  import defs._

  trait SelectionValidator extends UDTGenSiteParticipant { this: TypingTransformer with TreeGenerator with Logger =>

    protected def getUDT(tpe: Type): Either[String, (Tree, UDTDescriptor)] = {

      val udt = inferImplicitInst(mkUdtOf(tpe))
      val udtWithDesc = udt flatMap { ref => getUDTDescriptors(unit) get ref.symbol map ((ref, _)) }

      udtWithDesc.toRight("Missing UDT[" + tpe + "]")
    }

    protected def chkSelectors(udt: UDTDescriptor, sels: List[List[String]]): List[String] = {
      sels flatMap { sel => chkSelector(udt, sel.head, sel.tail) }
    }

    protected def chkSelector(udt: UDTDescriptor, path: String, sel: List[String]): Option[String] = (udt, sel) match {
      case (_: OpaqueDescriptor, _)           => None
      case (_, Nil) if udt.isPrimitiveProduct => None
      case (_, Nil)                           => Some(path + ": " + udt.tpe + " is not a primitive or product of primitives")
      case (_, field :: rest) => udt.select(field) match {
        case None      => Some("member " + field + " is not a case accessor of " + path + ": " + udt.tpe)
        case Some(udt) => chkSelector(udt, path + "." + field, rest)
      }
    }
  }
}