package eu.stratosphere.pact4s.compiler

import scala.tools.nsc.symtab.Flags._

trait TreeGeneration { this: Pact4sGlobal =>

  import global._

  trait TreeGenerator { this: TypingTransformer =>

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
      val lblName = localTyper.context.unit.freshTermName("while")
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