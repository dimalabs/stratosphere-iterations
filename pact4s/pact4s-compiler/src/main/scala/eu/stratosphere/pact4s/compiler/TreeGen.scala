package eu.stratosphere.pact4s.compiler

import scala.tools.nsc.symtab.Flags._

trait TreeGen { this: Pact4sGlobal =>

  import global._

  object defs {

    lazy val intArrayTpe = definitions.arrayType(definitions.IntClass.tpe)
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
        definitions.getClass("java.lang.Boolean") -> getBoxInfo(definitions.BooleanClass, "boolean", "Boolean"),
        definitions.getClass("java.lang.Byte") -> getBoxInfo(definitions.ByteClass, "byte", "Byte"),
        definitions.getClass("java.lang.Character") -> getBoxInfo(definitions.CharClass, "char", "Character"),
        definitions.getClass("java.lang.Double") -> getBoxInfo(definitions.DoubleClass, "double", "Double"),
        definitions.getClass("java.lang.Float") -> getBoxInfo(definitions.FloatClass, "float", "Float"),
        definitions.getClass("java.lang.Integer") -> getBoxInfo(definitions.IntClass, "int", "Integer"),
        definitions.getClass("java.lang.Long") -> getBoxInfo(definitions.LongClass, "long", "Long"),
        definitions.getClass("java.lang.Short") -> getBoxInfo(definitions.ShortClass, "short", "Short")
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
  }

  class TreeGenHelper(unit: CompilationUnit) {

    def mkIdent(target: Symbol): Tree = Ident(target) setType target.tpe

    def mkVar(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
      val valSym = owner.newValue(name) setFlag (flags | SYNTHETIC | MUTABLE) setInfo valTpe
      if (transient) valSym.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
      ValDef(valSym, value(valSym))
    }

    def mkVal(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
      val valSym = owner.newValue(name) setFlag (flags | SYNTHETIC) setInfo valTpe
      if (transient) valSym.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
      ValDef(valSym, value(valSym))
    }

    def mkValAndGetter(owner: Symbol, name: String, flags: Long, valTpe: Type)(value: Symbol => Tree): List[Tree] = {

      val valDef = mkVal(owner, name + " ", PRIVATE, false, valTpe) { value }

      val defDef = mkMethod(owner, name, flags | ACCESSOR, Nil, valTpe) { _ =>
        if (owner.isClass)
          Select(This(owner), name + " ")
        else
          Ident(valDef.symbol)
      }

      List(valDef, defDef)
    }

    def mkVarAndLazyGetter(owner: Symbol, name: String, flags: Long, valTpe: Type)(value: Symbol => Tree): List[Tree] = {

      val privateFlag = if (owner.isClass) PRIVATE else 0
      val varDef = mkVar(owner, name + " ", privateFlag, false, valTpe) { _ => Literal(Constant(null)) }

      val defDef = mkMethod(owner, name, privateFlag | flags | ACCESSOR, Nil, valTpe) { defSym =>

        def selVar = {
          if (owner.isClass)
            Select(This(owner), name + " ")
          else
            Ident(varDef.symbol)
        }

        val chk = Apply(Select(selVar, "$eq$eq"), List(Literal(Constant(null))))
        val init = Assign(selVar, value(defSym))
        Block(If(chk, init, EmptyTree), selVar)
      }

      List(varDef, defDef)
    }

    def mkWhile(cond: Tree)(body: Tree): Tree = {
      val lblName = unit.freshTermName("while")
      val jump = Apply(Ident(lblName), Nil)
      val block = body match {
        case Block(stats, expr) => new Block(stats :+ expr, jump)
        case _                  => new Block(List(body), jump)
      }
      LabelDef(lblName, Nil, If(cond, block, EmptyTree))
    }

    def mkIf(cond: Tree, bodyT: Tree*): Seq[Tree] = mkIf(cond, bodyT, Seq(EmptyTree))

    def mkIf(cond: Tree, bodyT: Seq[Tree], bodyF: Seq[Tree]): Seq[Tree] = cond match {
      case EmptyTree => bodyT
      case _         => Seq(If(cond, Block(bodyT: _*), Block(bodyF: _*)))
    }

    def mkAnd(cond1: Tree, cond2: Tree): Tree = cond1 match {
      case EmptyTree => cond2
      case _ => cond2 match {
        case EmptyTree => cond1
        case _         => gen.mkAnd(cond1, cond2)
      }
    }

    def mkMethod(owner: Symbol, name: String, flags: Long, args: List[(String, Type)], ret: Type)(impl: Symbol => Tree): Tree = {

      val methodSym = owner.newMethod(name) setFlag (flags | SYNTHETIC)

      if (args.isEmpty)
        methodSym setInfo NullaryMethodType(ret)
      else
        methodSym setInfo MethodType(methodSym newSyntheticValueParams args.unzip._2, ret)

      val valParams = args map { case (name, tpe) => ValDef(methodSym.newValueParameter(NoPosition, name) setInfo tpe) }

      DefDef(methodSym, Modifiers(flags | SYNTHETIC), List(valParams), impl(methodSym))
    }

    def mkClass(owner: Symbol, name: TypeName, flags: Long, parents: List[Type])(members: Symbol => List[Tree]): Tree = {

      val classSym = {
        if (name == null)
          owner newAnonymousClass owner.pos
        else
          owner newClass (owner.pos, name)
      }

      classSym setFlag (flags | SYNTHETIC)
      classSym setInfo ClassInfoType(parents, newScope, classSym)

      val classMembers = members(classSym)
      classMembers foreach { m => classSym.info.decls enter m.symbol }

      ClassDef(classSym, Modifiers(flags | SYNTHETIC), List(Nil), List(Nil), classMembers, owner.pos)
    }

    def mkThrow(msg: String) = Throw(New(TypeTree(definitions.getClass("java.lang.RuntimeException").tpe), List(List(Literal(msg)))))
  }
}