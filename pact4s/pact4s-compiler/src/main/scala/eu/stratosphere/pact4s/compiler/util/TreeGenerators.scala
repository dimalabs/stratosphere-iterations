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

import scala.tools.nsc.transform.TypingTransformers

trait TreeGenerators { this: TypingTransformers =>

  import global._

  trait TreeGenerator { this: TypingTransformer =>

    val Flags = scala.tools.nsc.symtab.Flags

    def freshTypeName(name: String) = localTyper.context.unit.freshTypeName(name)
    def freshTermName(name: String) = localTyper.context.unit.freshTermName(name)

    def mkUnit = Literal(Constant(()))
    def mkNull = Literal(Constant(null))
    def mkZero = Literal(Constant(0))
    def mkOne = Literal(Constant(1))

    def mkAsInstanceOf(source: Tree, targetTpe: Type): Tree = TypeApply(Select(source, "asInstanceOf"), List(TypeTree(targetTpe)))

    def maybeMkAsInstanceOf(source: Tree, sourceTpe: Type, targetTpe: Type): Tree = {
      if (sourceTpe <:< targetTpe)
        source
      else
        mkAsInstanceOf(source, targetTpe)
    }

    def mkIdent(target: Symbol): Tree = Ident(target) setType target.tpe

    def mkVar(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
      val valSym = owner.newValue(name) setFlag (flags | Flags.MUTABLE) setInfo valTpe
      if (transient) valSym.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
      ValDef(valSym, value(valSym))
    }

    def mkVal(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
      val valSym = owner.newValue(name) setFlag flags setInfo valTpe
      if (transient) valSym.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
      ValDef(valSym, value(valSym))
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

    def mkVarAndLazyGetter(owner: Symbol, name: String, flags: Long, valTpe: Type)(value: Symbol => Tree): List[Tree] = {

      val privateFlag = if (owner.isClass) Flags.PRIVATE else 0
      val varDef = mkVar(owner, name + " ", privateFlag, false, valTpe) { _ => Literal(Constant(null)) }

      val defDef = mkMethod(owner, name, privateFlag | flags | Flags.ACCESSOR, Nil, valTpe) { defSym =>

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
      val lblName = freshTermName("while")
      val jump = Apply(Ident(lblName), Nil)
      val block = body match {
        case Block(stats, expr) => new Block(stats :+ expr, jump)
        case _                  => new Block(List(body), jump)
      }
      LabelDef(lblName, Nil, If(cond, block, EmptyTree))
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

    def mkThrow(msg: String) = Throw(New(TypeTree(definitions.getClass("java.lang.RuntimeException").tpe), List(List(Literal(msg)))))
  }
}