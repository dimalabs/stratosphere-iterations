/**
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
 */

package eu.stratosphere.pact4s.compiler.udt.gen

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait UDTDeserializeMethodGenerators { this: Pact4sPlugin with UDTSerializerClassGenerators =>

  import global._
  import defs._

  trait UDTDeserializeMethodGenerator { this: UDTSerializerClassGenerator with TypingVisitor with UnitBoundTreeGenerator with Logger =>

    protected def mkDeserialize(udtSerClassSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

      val root = mkMethod(udtSerClassSym, "deserialize", Flags.OVERRIDE | Flags.FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { methodSym =>
        val env = GenEnvironment(udtSerClassSym, methodSym, listImpls, "flat" + desc.id, false, true, true)
        mkSingle(genDeserialize(desc, Ident("record"), env, Map()))
      }

      val aux = desc.getRecursiveRefs map { desc =>
        mkMethod(udtSerClassSym, "deserialize" + desc.id, Flags.PRIVATE | Flags.FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { methodSym =>
          val env = GenEnvironment(udtSerClassSym, methodSym, listImpls, "boxed" + desc.id, true, false, true)
          mkSingle(genDeserialize(desc, Ident("record"), env, Map()))
        }
      }

      root +: aux.toList
    }

    private def genDeserialize(desc: UDTDescriptor, source: Tree, env: GenEnvironment, scope: Map[Int, Symbol]): Seq[Tree] = desc match {

      case PrimitiveDescriptor(id, _, default, _) => {
        val chk = env.mkChkIdx(id)
        val des = env.mkGetFieldInto(id, source)
        val get = env.mkGetValue(id)

        Seq(mkIf(chk, Block(des, get), default))
      }

      case BoxedPrimitiveDescriptor(id, tpe, _, _, box, _) => {
        val des = env.mkGetFieldInto(id, source)
        val chk = mkAnd(env.mkChkIdx(id), des)
        val get = box(env.mkGetValue(id))

        Seq(mkIf(chk, get, mkNull))
      }

      case list @ ListDescriptor(id, tpe, _, cbf, _, elem) => {
        val chk = mkAnd(env.mkChkIdx(id), env.mkNotIsNull(id, source))

        val (init, list) = env.reentrant match {

          // This is a bit conservative, but avoids runtime checks
          // and/or even more specialized deserialize() methods to
          // track whether it's safe to reuse the list variable.
          case true => {
            val listTpe = env.listImpls(id)
            val list = mkVal(env.methodSym, "list" + id, 0, false, listTpe) { _ => New(TypeTree(listTpe), List(List())) }
            (list, Ident(list.symbol))
          }

          case false => {
            val clear = Apply(Select(env.mkSelectWrapper(id), "clear"), List())
            (clear, env.mkSelectWrapper(id))
          }
        }

        val buildTpe = appliedType(builderClass.tpe, List(elem.tpe, tpe))
        val build = mkVal(env.methodSym, "b" + id, 0, false, buildTpe) { _ => Apply(Select(cbf(), "apply"), List()) }
        val des = env.mkGetFieldInto(id, source, list)
        val body = genDeserializeList(elem, list, Ident(build.symbol), env.copy(chkNull = true), scope)
        val stats = init +: des +: build +: body

        Seq(mkIf(chk, Block(stats: _*), mkNull))
      }

      // we have a mutable UDT
      case CaseClassDescriptor(_, tpe, true, _, _, getters) => {

        val fields = getters filterNot { _.isBaseField } map {
          case FieldAccessor(_, _, _, _, desc) => desc.id -> mkVal(env.methodSym, "v" + desc.id, 0, false, desc.tpe) { _ =>
            mkSingle(genDeserialize(desc, source, env, scope))
          }
        }

        val newScope = scope ++ (fields map { case (id, tree) => id -> tree.symbol })

        val stats = fields map { _._2 }

        val setterStats = getters map {
          case FieldAccessor(_, setter, fTpe, _, fDesc) => {
            val sym = newScope(fDesc.id)
            val castVal = maybeMkAsInstanceOf(Ident(sym), sym.tpe, fTpe.resultType)
            env.mkCallSetMutableField(desc.id, setter, castVal)
          }
        }

        val ret = env.mkSelectMutableUdtInst(desc.id)

        (stats ++ setterStats) :+ ret
      }

      case CaseClassDescriptor(_, tpe, _, _, _, getters) => {

        val fields = getters filterNot { _.isBaseField } map {
          case FieldAccessor(_, _, _, _, desc) => desc.id -> mkVal(env.methodSym, "v" + desc.id, 0, false, desc.tpe) { _ =>
            mkSingle(genDeserialize(desc, source, env, scope))
          }
        }

        val newScope = scope ++ (fields map { case (id, tree) => id -> tree.symbol })

        val stats = fields map { _._2 }

        val args = getters map {
          case FieldAccessor(_, _, fTpe, _, desc) => {
            val sym = newScope(desc.id)
            maybeMkAsInstanceOf(Ident(sym), sym.tpe, fTpe.resultType)
          }
        }

        val ret = New(TypeTree(tpe), List(args.toList))

        stats :+ ret
      }

      case BaseClassDescriptor(_, tpe, Seq(tagField, baseFields @ _*), subTypes) => {

        val fields = baseFields map {
          case FieldAccessor(_, _, _, _, desc) => desc.id -> mkVal(env.methodSym, "v" + desc.id, 0, false, desc.tpe) { _ =>
            val special = desc match {
              case d @ PrimitiveDescriptor(id, _, _, _) if id == tagField.desc.id => d.copy(default = Literal(-1))
              case _ => desc
            }
            mkSingle(genDeserialize(desc, source, env, scope))
          }
        }

        val newScope = scope ++ (fields map { case (id, tree) => id -> tree.symbol })

        val stats = fields map { _._2 }

        val cases = subTypes.zipWithIndex.toList map {
          case (dSubType, i) => {
            val code = mkSingle(genDeserialize(dSubType, source, env, newScope))
            val pat = Bind("tag", Literal(i))
            CaseDef(pat, EmptyTree, code)
          }
        }

        val chk = env.mkChkIdx(tagField.desc.id)
        val des = env.mkGetFieldInto(tagField.desc.id, source)
        val get = env.mkGetValue(tagField.desc.id)

        Seq(mkIf(chk, Block((stats :+ des :+ Match(get, cases)): _*), mkNull))
      }

      case OpaqueDescriptor(id, tpe, _) => Seq(Apply(Select(env.mkSelectSerializer(id), "deserialize"), List(source)))

      case RecursiveDescriptor(id, tpe, refId) => {
        val chk = mkAnd(env.mkChkIdx(id), env.mkNotIsNull(id, source))
        val rec = mkVal(env.methodSym, "record" + id, 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
        val get = env.mkGetFieldInto(id, source, Ident(rec.symbol))
        val des = env.mkCallDeserialize(refId, Ident(rec.symbol))

        Seq(mkIf(chk, Block(rec, get, des), mkNull))
      }

      case _ => Seq(mkNull)
    }

    private def genDeserializeList(elem: UDTDescriptor, source: Tree, target: Tree, env: GenEnvironment, scope: Map[Int, Symbol]): Seq[Tree] = {

      val size = mkVal(env.methodSym, "size", 0, false, intTpe) { _ => Apply(Select(source, "size"), List()) }
      val sizeHint = Apply(Select(target, "sizeHint"), List(Ident(size.symbol)))
      val i = mkVar(env.methodSym, "i", 0, false, intTpe) { _ => mkZero }

      val loop = mkWhile(Apply(Select(Ident(i.symbol), "$less"), List(Ident(size.symbol)))) {

        val item = mkVal(env.methodSym, "item", 0, false, getListElemWrapperType(elem, env)) { _ => Apply(Select(source, "get"), List(Ident(i.symbol))) }

        val (stats, value) = elem match {

          case PrimitiveDescriptor(_, _, _, wrapper)              => (Seq(), env.mkGetValue(Ident(item.symbol)))

          case BoxedPrimitiveDescriptor(_, _, _, wrapper, box, _) => (Seq(), box(env.mkGetValue(Ident(item.symbol))))

          case ListDescriptor(id, tpe, _, cbf, _, innerElem) => {

            val buildTpe = appliedType(builderClass.tpe, List(innerElem.tpe, tpe))
            val build = mkVal(env.methodSym, "b" + id, 0, false, buildTpe) { _ => Apply(Select(cbf(), "apply"), List()) }
            val body = mkVal(env.methodSym, "v" + id, 0, false, elem.tpe) { _ => mkSingle(genDeserializeList(innerElem, Ident(item.symbol), Ident(build.symbol), env, scope)) }
            (Seq(build, body), Ident(body.symbol))
          }

          case RecursiveDescriptor(id, tpe, refId) => (Seq(), env.mkCallDeserialize(refId, Ident(item.symbol)))

          case _ => {
            val body = genDeserialize(elem, Ident(item.symbol), env.copy(idxPrefix = "boxed" + elem.id, chkIndex = false, chkNull = false), scope)
            val v = mkVal(env.methodSym, "v" + elem.id, 0, false, elem.tpe) { _ => mkSingle(body) }
            (Seq(v), Ident(v.symbol))
          }
        }

        val chk = env.mkChkNotNull(Ident(item.symbol), elem.tpe)
        val add = Apply(Select(target, "$plus$eq"), List(value))
        val addNull = Apply(Select(target, "$plus$eq"), List(mkNull))
        val inc = Assign(Ident(i.symbol), Apply(Select(Ident(i.symbol), "$plus"), List(mkOne)))

        Block(item, mkIf(chk, mkSingle(stats :+ add), addNull), inc)
      }

      val get = Apply(Select(target, "result"), List())

      Seq(size, sizeHint, i, loop, get)
    }

    private def getListElemWrapperType(desc: UDTDescriptor, env: GenEnvironment): Type = desc match {
      case PrimitiveDescriptor(_, _, _, wrapper) => wrapper.tpe
      case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, _) => wrapper.tpe
      case ListDescriptor(id, _, _, _, _, _) => env.listImpls(id)
      case _ => pactRecordClass.tpe
    }
  }
}