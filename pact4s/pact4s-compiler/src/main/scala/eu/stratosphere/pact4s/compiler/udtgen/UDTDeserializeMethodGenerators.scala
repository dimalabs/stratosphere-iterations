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

trait UDTDeserializeMethodGenerators { this: Pact4sPlugin with UDTSerializerClassGenerators =>

  import global._
  import defs._

  trait UDTDeserializeMethodGenerator { this: TreeGenerator with UDTSerializerClassGenerator =>

    protected def mkDeserialize(udtSerClassSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

      val root = mkMethod(udtSerClassSym, "deserialize", Flags.OVERRIDE | Flags.FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { methodSym =>
        val env = GenEnvironment(udtSerClassSym, methodSym, listImpls, "flat" + desc.id, false, true, true)
        mkSingle(genDeserialize(desc, Ident("record"), env, Map()))
      }

      val aux = (desc.findByType[RecursiveDescriptor].toList flatMap { rd => desc.findById(rd.refId) } distinct) map { desc =>
        mkMethod(udtSerClassSym, "deserialize" + desc.id, Flags.PRIVATE | Flags.FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { methodSym =>
          val env = GenEnvironment(udtSerClassSym, methodSym, listImpls, "boxed" + desc.id, true, false, true)
          mkSingle(genDeserialize(desc, Ident("record"), env, Map()))
        }
      }

      root +: aux
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

      case CaseClassDescriptor(_, tpe, _, _, getters) => {

        val fields = getters filterNot { _.isBaseField } map {
          case FieldAccessor(_, _, _, desc) => desc.id -> mkVal(env.methodSym, "v" + desc.id, 0, false, desc.tpe) { _ =>
            mkSingle(genDeserialize(desc, source, env, scope))
          }
        } toMap

        val newScope = scope ++ (fields map { case (id, tree) => id -> tree.symbol })

        val stats = fields.toSeq map { _._2 }
        val args = getters map { case FieldAccessor(_, _, _, desc) => Ident(newScope(desc.id)) } toList
        val ret = New(TypeTree(tpe), List(args))

        Seq(mkSingle(stats :+ ret))
      }

      case BaseClassDescriptor(_, tpe, getters @ Seq(tagField, baseFields @ _*), subTypes) => {

        val fields = getters map {
          case FieldAccessor(_, _, _, desc) => desc.id -> mkVal(env.methodSym, "v" + desc.id, 0, false, desc.tpe) { _ =>
            val special = desc match {
              case d @ PrimitiveDescriptor(id, _, _, _) if id == tagField.desc.id => d.copy(default = Literal(-1))
              case _ => desc
            }
            mkSingle(genDeserialize(special, source, env, scope))
          }
        } toMap

        val newScope = scope ++ (fields map { case (id, tree) => id -> tree.symbol })

        val stats = fields.toSeq map { _._2 }

        val cases = subTypes.zipWithIndex.toList map {
          case (dSubType, i) => {
            val code = mkSingle(genDeserialize(dSubType, source, env, newScope))
            val pat = Bind("tag", Literal(i))
            CaseDef(pat, EmptyTree, code)
          }
        }

        val noTagCase = CaseDef(Bind("tag", Literal(-1)), EmptyTree, mkNull)

        stats :+ Match(Ident(newScope(tagField.desc.id)), noTagCase +: cases)
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

    private def genDeserializeListElem(elem: UDTDescriptor, bf: Tree, target: Tree, env: GenEnvironment): Seq[Tree] = Seq()
  }
}