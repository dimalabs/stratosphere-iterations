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

package eu.stratosphere.pact4s.compiler.util

trait TreeGenerators { this: HasGlobal =>

  import global._

  trait TreeGenerator {

    val Flags = scala.tools.nsc.symtab.Flags

    def mkDefault(classSym: Symbol): Tree = {
      import definitions._
      classSym match {
        case BooleanClass => Literal(false)
        case ByteClass    => Literal(0: Byte)
        case CharClass    => Literal(0: Char)
        case DoubleClass  => Literal(0: Double)
        case FloatClass   => Literal(0: Float)
        case IntClass     => Literal(0: Int)
        case LongClass    => Literal(0: Long)
        case ShortClass   => Literal(0: Short)
        case UnitClass    => Literal(())
        case _            => Literal(null: Any)
      }
    }

    def mkUnit = Literal(Constant(())) setType definitions.UnitClass.tpe
    def mkNull = Literal(Constant(null)) setType ConstantType(Constant(null))
    def mkZero = Literal(Constant(0)) setType ConstantType(Constant(0))
    def mkOne = Literal(Constant(1)) setType ConstantType(Constant(1))

    def mkAsInstanceOf(source: Tree, targetTpe: Type): Tree = TypeApply(Select(source, "asInstanceOf"), List(TypeTree(targetTpe)))

    def maybeMkAsInstanceOf(source: Tree, sourceTpe: Type, targetTpe: Type): Tree = {
      if (sourceTpe <:< targetTpe)
        source
      else
        mkAsInstanceOf(source, targetTpe)
    }

    def mkIdent(target: Symbol): Tree = Ident(target) setType target.tpe
    def mkSelect(rootModule: String, path: String*): Tree = mkSelect(Ident(rootModule) setSymbol definitions.getModule(rootModule), path: _*)
    def mkSelect(source: Tree, path: String*): Tree = path.foldLeft(source) { (ret, item) => Select(ret, item) }
    def mkSelectSyms(source: Tree, path: Symbol*): Tree = path.foldLeft(source) { (ret, item) => Select(ret, item) }

    def mkSeq(items: List[Tree]): Tree = Apply(mkSelect("scala", "collection", "Seq", "apply"), items)
    def mkList(items: List[Tree]): Tree = Apply(mkSelect("scala", "collection", "immutable", "List", "apply"), items)

    def mkVal(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
      val valSym = owner.newValue(name) setFlag flags setInfo valTpe
      if (transient) valSym.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
      ValDef(valSym, value(valSym))
    }

    def mkVar(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
      mkVal(owner, name, flags | Flags.MUTABLE, transient, valTpe)(value)
    }

    def mkValAndGetter(owner: Symbol, name: String, flags: Long, valTpe: Type)(value: Symbol => Tree): List[Tree] = {

      val valDef = mkVal(owner, name + " ", Flags.PRIVATE, false, valTpe) { value }

      val defDef = mkMethod(owner, name, flags | Flags.ACCESSOR, Nil, valTpe) { _ =>
        if (owner.isClass)
          Select(This(owner), name + " ")
        else
          Ident(valDef.symbol)
      }

      List(valDef, defDef)
    }

    def mkVarAndLazyGetter(owner: Symbol, name: String, flags: Long, valTpe: Type)(value: Symbol => Tree): (Tree, Tree) = {

      // Class-member vars without setters must have the local flag, but
      // this flag causes an error when type checking the field and getter
      // individually. This method is extremely anal about setting symbols
      // and types on the generated trees so that they don't have to be
      // explicitly run through the type checker. For some reason, the type 
      // checker doesn't set type info on these trees when processing the 
      // parent ClassDef. 

      val varFlags = if (owner.isClass) Flags.PRIVATE | Flags.LOCAL else 0
      val field = mkVar(owner, name + " ", varFlags, false, valTpe) { _ => mkNull }

      val getter = mkMethod(owner, name, flags, Nil, valTpe) { defSym =>

        def selField = {
          val tree = owner.isClass match {
            case true  => Select(This(owner) setType owner.thisType, name + " ")
            case false => Ident(field.symbol)
          }
          tree setSymbol field.symbol setType valTpe
        }

        val eqeqSym = definitions.Object_==
        val eqeq = Select(selField, "$eq$eq") setSymbol eqeqSym setType eqeqSym.tpe
        val chk = Apply(eqeq, List(mkNull)) setType definitions.BooleanClass.tpe
        val init = Assign(selField, value(defSym)) setType definitions.UnitClass.tpe
        Block(If(chk, init, EmptyTree) setType definitions.UnitClass.tpe, selField) setType valTpe
      }

      (field setType NoType, getter setType NoType)
    }

    def mkIf(cond: Tree, bodyT: Tree): Tree = mkIf(cond, bodyT, EmptyTree)

    def mkIf(cond: Tree, bodyT: Tree, bodyF: Tree): Tree = cond match {
      case EmptyTree => bodyT
      case _         => If(cond, bodyT, bodyF)
    }

    def mkSingle(stats: Seq[Tree]): Tree = stats match {
      case Seq()     => EmptyTree
      case Seq(stat) => stat
      case _         => Block(stats: _*)
    }

    def mkAnd(cond1: Tree, cond2: Tree): Tree = cond1 match {
      case EmptyTree => cond2
      case _ => cond2 match {
        case EmptyTree => cond1
        case _         => gen.mkAnd(cond1, cond2)
      }
    }

    def mkMethod(owner: Symbol, name: String, flags: Long, args: List[(String, Type)], ret: Type)(impl: Symbol => Tree): Tree = {

      val methodSym = owner.newMethod(name) setFlag (flags | Flags.SYNTHETIC)

      if (args.isEmpty)
        methodSym setInfo NullaryMethodType(ret)
      else
        methodSym setInfo MethodType(methodSym newSyntheticValueParams args.unzip._2, ret)

      val valParams = args map { case (name, tpe) => ValDef(methodSym.newValueParameter(NoPosition, name) setInfo tpe) }

      DefDef(methodSym, Modifiers(flags | Flags.SYNTHETIC), List(valParams), impl(methodSym))
    }

    def mkClass(owner: Symbol, name: TypeName, flags: Long, parents: List[Type])(members: Symbol => List[Tree]): Tree = {

      val classSym = {
        if (name == null)
          owner newAnonymousClass owner.pos
        else
          owner newClass (owner.pos, name)
      }

      classSym setFlag (flags | Flags.SYNTHETIC)
      classSym setInfo ClassInfoType(parents, newScope, classSym)

      val classMembers = members(classSym)
      classMembers foreach { m => classSym.info.decls enter m.symbol }

      ClassDef(classSym, Modifiers(flags | Flags.SYNTHETIC), List(Nil), List(Nil), classMembers, owner.pos)
    }

    def mkThrow(tpe: Type, msg: Tree): Tree = Throw(New(TypeTree(tpe), List(List(msg))))
    def mkThrow(tpe: Type, msg: String): Tree = mkThrow(tpe, Literal(msg))
    def mkThrow(msg: String): Tree = mkThrow(definitions.getClass("java.lang.RuntimeException").tpe, msg)

    implicit def tree2Ops[T <: Tree](tree: T) = new {
      // copy of Tree.copyAttrs, since that method is private
      def copyAttrs(from: Tree): T = {
        tree.pos = from.pos
        tree.tpe = from.tpe
        if (tree.hasSymbol) tree.symbol = from.symbol
        tree
      }

      def getSimpleClassName: String = {
        val name = tree.getClass.getName
        val idx = math.max(name.lastIndexOf('$'), name.lastIndexOf('.')) + 1
        name.substring(idx)
      }
    }
  }

  trait UnitBoundTreeGenerator extends TreeGenerator { this: HasCompilationUnit =>

    def mkWhile(cond: Tree)(body: Tree): Tree = {
      val lblName = unit.freshTermName("while")
      val jump = Apply(Ident(lblName), Nil)
      val block = body match {
        case Block(stats, expr) => new Block(stats :+ expr, jump)
        case _                  => new Block(List(body), jump)
      }
      LabelDef(lblName, Nil, If(cond, block, EmptyTree))
    }
  }
}

