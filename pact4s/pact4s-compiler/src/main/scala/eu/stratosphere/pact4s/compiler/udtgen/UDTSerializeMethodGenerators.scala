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

package eu.stratosphere.pact4s.compiler.udtgen

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait UDTSerializeMethodGenerators { this: Pact4sPlugin with UDTSerializerClassGenerators =>

  import global._
  import defs._

  trait UDTSerializeMethodGenerator { this: TreeGenerator with UDTSerializerClassGenerator =>

    protected def mkSerialize(udtSerClassSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

      val root = mkMethod(udtSerClassSym, "serialize", Flags.OVERRIDE | Flags.FINAL, List(("item", desc.tpe), ("record", pactRecordClass.tpe)), definitions.UnitClass.tpe) { methodSym =>
        val env = GenEnvironment(udtSerClassSym, methodSym, listImpls, "flat" + desc.id, false, true, true)
        val stats = genSerialize(desc, Ident("item"), Ident("record"), env)
        Block(stats.toList, mkUnit)
      }

      val aux = (desc.findByType[RecursiveDescriptor].toList flatMap { rd => desc.findById(rd.refId) } distinct) map { desc =>
        mkMethod(udtSerClassSym, "serialize" + desc.id, Flags.PRIVATE | Flags.FINAL, List(("item", desc.tpe), ("record", pactRecordClass.tpe)), definitions.UnitClass.tpe) { methodSym =>
          val env = GenEnvironment(udtSerClassSym, methodSym, listImpls, "boxed" + desc.id, true, true, true)
          val stats = genSerialize(desc, Ident("item"), Ident("record"), env)
          Block(stats.toList, mkUnit)
        }
      }

      root +: aux
    }

    private def genSerialize(desc: UDTDescriptor, source: Tree, target: Tree, env: GenEnvironment): Seq[Tree] = desc match {

      case PrimitiveDescriptor(id, _, _, _) => {
        val chk = env.mkChkIdx(id)
        val ser = env.mkSetValue(id, source)
        val set = env.mkSetField(id, target)

        mkIf(chk, ser, set)
      }

      case BoxedPrimitiveDescriptor(id, tpe, _, _, _, unbox) => {
        val chk = mkAnd(env.mkChkIdx(id), env.mkChkNotNull(source, tpe))
        val ser = env.mkSetValue(id, unbox(source))
        val set = env.mkSetField(id, target)

        mkIf(chk, ser, set)
      }

      case list @ ListDescriptor(id, tpe, _, _, iter, elem) => {
        val chk = mkAnd(env.mkChkIdx(id), env.mkChkNotNull(source, tpe))

        val upd = list.getInnermostElem match {
          case _: RecursiveDescriptor => Some(Apply(Select(target, "updateBinaryRepresenation"), List()))
          case _                      => None
        }

        val stats = env.reentrant match {

          // This is a bit conservative, but avoids runtime checks
          // and/or even more specialized serialize() methods to
          // track whether it's safe to reuse the list variable.
          case true => {
            val listTpe = env.listImpls(id)
            val list = mkVal(env.methodSym, "list" + id, 0, false, listTpe) { _ => New(TypeTree(listTpe), List(List())) }
            val body = genSerializeListElem(elem, iter(source), Ident(list.symbol), env.copy(chkNull = true))
            val set = env.mkSetField(id, target, Ident(list.symbol))
            (list +: body) :+ set
          }

          case false => {
            val clear = Apply(Select(env.mkSelectWrapper(id), "clear"), List())
            val body = genSerializeListElem(elem, iter(source), env.mkSelectWrapper(id), env.copy(chkNull = true))
            val set = env.mkSetField(id, target)
            (clear +: body) :+ set
          }
        }

        mkIf(chk, (upd.toSeq ++ stats): _*)
      }

      case CaseClassDescriptor(_, tpe, _, _, getters) => {
        val chk = env.mkChkNotNull(source, tpe)
        val stats = getters filterNot { _.isBaseField } flatMap { case FieldAccessor(sym, _, _, desc) => genSerialize(desc, Select(source, sym), target, env.copy(chkNull = true)) }

        mkIf(chk, stats: _*)
      }

      case BaseClassDescriptor(id, tpe, Seq(tagField, baseFields @ _*), subTypes) => {
        val chk = env.mkChkNotNull(source, tpe)
        val fields = baseFields flatMap { (f => genSerialize(f.desc, Select(source, f.sym), target, env.copy(chkNull = true))) }
        val cases = subTypes.zipWithIndex.toList map {
          case (dSubType, i) => {
            val tag = genSerialize(tagField.desc, Literal(i), target, env.copy(chkNull = false))
            val code = genSerialize(dSubType, Ident("inst"), target, env.copy(chkNull = false))
            val body = (tag ++ code) :+ mkUnit

            val pat = Bind("inst", Typed(Ident("_"), TypeTree(dSubType.tpe)))
            CaseDef(pat, EmptyTree, Block(body: _*))
          }
        }

        mkIf(chk, (fields :+ Match(source, cases)): _*)
      }

      case OpaqueDescriptor(id, tpe, _) => {
        val ser = Apply(Select(env.mkSelectSerializer(id), "serialize"), List(source, target))
        mkIf(env.mkChkNotNull(source, tpe), ser)
      }

      case RecursiveDescriptor(id, tpe, refId) => {
        // Important: recursive types introduce re-entrant calls to serialize()

        val chk = mkAnd(env.mkChkIdx(id), env.mkChkNotNull(source, tpe))

        // Persist the outer record prior to recursing, since the call
        // is going to reuse all the PactPrimitive wrappers that were 
        // needed *before* the recursion.
        val updTgt = Apply(Select(target, "updateBinaryRepresenation"), List())

        val rec = mkVal(env.methodSym, "record" + id, 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
        val ser = env.mkCallSerialize(refId, source, Ident(rec.symbol))

        // Persist the new inner record after recursing, since the
        // current call is going to reuse all the PactPrimitive
        // wrappers that are needed *after* the recursion.
        val updRec = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())

        val set = env.mkSetField(id, target, Ident(rec.symbol))

        mkIf(chk, updTgt, rec, ser, updRec, set)
      }
    }

    private def genSerializeListElem(elem: UDTDescriptor, iter: Tree, target: Tree, env: GenEnvironment): Seq[Tree] = {

      val it = mkVal(env.methodSym, "it", 0, false, mkIteratorOf(elem.tpe)) { _ => iter }

      val loop = mkWhile(Select(Ident(it.symbol), "hasNext")) {

        val item = mkVal(env.methodSym, "item", 0, false, elem.tpe) { _ => Select(Ident(it.symbol), "next") }

        val (stats, value) = elem match {

          case PrimitiveDescriptor(_, _, _, wrapper)                => (Seq(), New(TypeTree(wrapper.tpe), List(List(Ident(item.symbol)))))

          case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, unbox) => (Seq(), New(TypeTree(wrapper.tpe), List(List(unbox(Ident(item.symbol))))))

          case ListDescriptor(id, _, _, _, iter, innerElem) => {
            val listTpe = env.listImpls(id)
            val list = mkVal(env.methodSym, "list" + id, 0, false, listTpe) { _ => New(TypeTree(listTpe), List(List())) }
            val body = genSerializeListElem(innerElem, iter(Ident(item.symbol)), Ident(list.symbol), env)
            (list +: body, Ident(list.symbol))
          }

          case RecursiveDescriptor(id, tpe, refId) => {
            val rec = mkVal(env.methodSym, "record" + id, 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
            val ser = env.mkCallSerialize(refId, Ident(item.symbol), Ident(rec.symbol))
            val updRec = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())

            (Seq(rec, ser, updRec), Ident(rec.symbol))
          }

          case _ => {
            val rec = mkVal(env.methodSym, "record", 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
            val ser = genSerialize(elem, Ident(item.symbol), Ident(rec.symbol), env.copy(idxPrefix = "boxed" + elem.id, chkIndex = false, chkNull = false))
            val upd = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())
            ((rec +: ser) :+ upd, Ident(rec.symbol))
          }
        }

        val chk = env.mkChkNotNull(Ident(item.symbol), elem.tpe)
        val add = Apply(Select(target, "add"), List(value))
        val addNull = Apply(Select(target, "add"), List(mkNull))
        val body = item +: mkIf(chk, stats :+ add, Seq(addNull))

        Block(body: _*)
      }

      Seq(it, loop)
    }
  }
}

