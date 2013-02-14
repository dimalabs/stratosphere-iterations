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

trait UDTSerializerClassGenerators extends UDTSerializeMethodGenerators with UDTDeserializeMethodGenerators { this: Pact4sPlugin =>

  import global._
  import defs._

  trait UDTSerializerClassGenerator extends UDTSerializeMethodGenerator with UDTDeserializeMethodGenerator { this: UDTClassGenerator with TypingVisitor with UnitBoundTreeGenerator with Logger =>

    protected def mkUdtSerializerClass(owner: Symbol, desc: UDTDescriptor): Tree = {

      mkClass(owner, unit.freshTypeName("UDTSerializerImpl"), Flags.FINAL, List(mkUdtSerializerOf(desc.tpe), definitions.SerializableClass.tpe)) { classSym =>

        val (listImpls, listImplTypes) = mkListImplClasses(classSym, desc)

        val indexMapIter = Select(Apply(Select(Select(Ident("scala"), "Predef"), "intArrayOps"), List(Ident("indexMap"))), "iterator")
        val (fields1, inits1) = mkIndexes(classSym, desc.id, getIndexFields(desc).toList, false, indexMapIter)
        val (fields2, inits2) = mkBoxedIndexes(classSym, desc)

        val fields = fields1 ++ fields2
        val init = inits1 ++ inits2 match {
          case Nil   => Nil
          case inits => List(mkMethod(classSym, "init", Flags.OVERRIDE | Flags.FINAL, List(), definitions.UnitClass.tpe) { _ => Block((inits :+ mkUnit): _*) })
        }

        val (wrapperFields, wrappers) = mkPactWrappers(classSym, desc, listImplTypes)

        val mutableUdts = desc.flatten.toList flatMap {
          case cc @ CaseClassDescriptor(_, _, true, _, _, _) => Some(cc)
          case _ => None
        } distinct

        val mutableUdtInsts = mutableUdts map { u => mkMutableUdtInst(classSym, u) }

        val helpers = listImpls ++ fields ++ wrapperFields ++ mutableUdtInsts ++ mkReadObject(classSym, wrappers, mutableUdts) ++ init
        val methods = mkGetFieldIndex(classSym, desc) :: mkSerialize(classSym, desc, listImplTypes) ++ mkDeserialize(classSym, desc, listImplTypes)

        helpers ++ methods
      }
    }

    private def mkListImplClasses(udtSerClassSym: Symbol, desc: UDTDescriptor): (List[Tree], Map[Int, Type]) = {

      def mkListImplClass(elemTpe: Type): Tree = mkClass(udtSerClassSym, unit.freshTypeName("PactListImpl"), Flags.FINAL, List(mkPactListOf(elemTpe))) { _ => Nil }

      def getListTypes(desc: UDTDescriptor): Seq[(Int, Int, Type)] = desc match {
        case ListDescriptor(id, _, _, _, _, elem: ListDescriptor) => {
          val impls @ Seq((_, depth, primTpe), _*) = getListTypes(elem)
          (id, depth + 1, primTpe) +: impls
        }
        case ListDescriptor(id, _, _, _, _, elem: PrimitiveDescriptor) => Seq((id, 1, elem.wrapper.tpe))
        case ListDescriptor(id, _, _, _, _, elem: BoxedPrimitiveDescriptor) => Seq((id, 1, elem.wrapper.tpe))
        case ListDescriptor(id, _, _, _, _, elem) => (id, 1, pactRecordClass.tpe) +: getListTypes(elem)
        case BaseClassDescriptor(_, _, getters, subTypes) => (getters flatMap { f => getListTypes(f.desc) }) ++ (subTypes flatMap getListTypes)
        case CaseClassDescriptor(_, _, _, _, _, getters) => getters flatMap { f => getListTypes(f.desc) }
        case _ => Seq()
      }

      val lists = (getListTypes(desc) groupBy { case (_, _, elemTpe) => elemTpe } toList) map {
        case (elemTpe, listTypes) => {

          val byDepth = listTypes groupBy { case (_, depth, _) => depth } mapValues { _.map { _._1 } }
          val (_, flatListIds :: nestedListIds) = byDepth.toList sortBy { case (depth, _) => depth } unzip

          val initImpl = mkListImplClass(elemTpe)
          val initMap = flatListIds map { _ -> initImpl.symbol.tpe }

          nestedListIds.foldLeft((List(initImpl), initMap)) { (result, listIds) =>
            val (impls, m) = result
            val listTpe = impls.head.symbol.tpe
            val impl = mkListImplClass(listTpe)
            (impl :: impls, m ++ (listIds map { _ -> impl.symbol.tpe }))
          }
        }
      }

      val (listImpls, listTypes) = lists.unzip
      (listImpls.flatten, listTypes.flatten.toMap)
    }

    private def mkIndexes(udtSerClassSym: Symbol, descId: Int, descFields: List[UDTDescriptor], boxed: Boolean, indexMapIter: Tree): (List[Tree], List[Tree]) = {

      val prefix = (if (boxed) "boxed" else "flat") + descId
      val iterName = prefix + "Iter"
      val iter = mkVal(udtSerClassSym, iterName, Flags.PRIVATE, true, mkIteratorOf(definitions.IntClass.tpe)) { _ => indexMapIter }

      val fieldsAndInits = descFields map {

        case OpaqueDescriptor(id, tpe, ref) => {
          val arr = Apply(Select(ref(), "mkIndexMap"), List(Select(This(udtSerClassSym), iterName)))
          val idxField = mkVal(udtSerClassSym, prefix + "Idx" + id, Flags.PRIVATE, true, intArrayTpe) { _ => arr }

          val serField = mkVar(udtSerClassSym, prefix + "Ser" + id, Flags.PRIVATE, false, mkUdtSerializerOf(tpe)) { _ => mkNull }

          val serInst = Apply(Select(ref(), "getSerializer"), List(Select(This(udtSerClassSym), prefix + "Idx" + id)))
          val serInit = Assign(Select(This(udtSerClassSym), prefix + "Ser" + id), serInst)

          (List(idxField, serField), List(serInit: Tree))
        }

        case d => {
          val next = Apply(Select(Select(This(udtSerClassSym), iterName), "next"), Nil)
          val idxField = mkVal(udtSerClassSym, prefix + "Idx" + d.id, Flags.PRIVATE, false, definitions.IntClass.tpe) { _ => next }

          (List(idxField), Nil)
        }
      }

      val (fields, inits) = fieldsAndInits.unzip
      (iter +: fields.flatten, inits.flatten)
    }

    private def mkBoxedIndexes(udtSerClassSym: Symbol, desc: UDTDescriptor): (List[Tree], List[Tree]) = {

      def getBoxedDescriptors(d: UDTDescriptor): Seq[UDTDescriptor] = d match {
        case ListDescriptor(_, _, _, _, _, elem: BaseClassDescriptor) => elem +: getBoxedDescriptors(elem)
        case ListDescriptor(_, _, _, _, _, elem: CaseClassDescriptor) => elem +: getBoxedDescriptors(elem)
        case ListDescriptor(_, _, _, _, _, elem: OpaqueDescriptor) => Seq(elem)
        case ListDescriptor(_, _, _, _, _, elem) => getBoxedDescriptors(elem)
        case CaseClassDescriptor(_, _, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getBoxedDescriptors(f.desc) }
        case BaseClassDescriptor(id, _, getters, subTypes) => (getters flatMap { f => getBoxedDescriptors(f.desc) }) ++ (subTypes flatMap getBoxedDescriptors)
        case RecursiveDescriptor(_, _, refId) => desc.findById(refId).map(_.mkRoot).toSeq
        case _ => Seq()
      }

      val fieldsAndInits = getBoxedDescriptors(desc).distinct.toList flatMap { d =>
        getIndexFields(d).toList match {
          case Nil => None
          case fields => {
            val widths = fields map {
              case OpaqueDescriptor(_, _, ref) => Select(ref(), "numFields")
              case _                           => mkOne
            }
            val sum = widths.reduce { (s, i) => Apply(Select(s, "$plus"), List(i)) }
            val range = Apply(Select(Ident("scala"), "Range"), List(mkZero, sum))
            Some(mkIndexes(udtSerClassSym, d.id, fields, true, Select(range, "iterator")))
          }
        }
      }

      val (fields, inits) = fieldsAndInits.unzip
      (fields.flatten, inits.flatten)
    }

    private def mkPactWrappers(udtSerClassSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type]): (List[Tree], List[(Int, Type)]) = {

      def getFieldTypes(desc: UDTDescriptor): Seq[(Int, Type)] = desc match {
        case PrimitiveDescriptor(id, _, _, wrapper)            => Seq((id, wrapper.tpe))
        case BoxedPrimitiveDescriptor(id, _, _, wrapper, _, _) => Seq((id, wrapper.tpe))
        case d @ ListDescriptor(id, _, _, _, _, elem) => {
          val listField = (id, listImpls(id))
          val elemFields = d.getInnermostElem match {
            case elem: CaseClassDescriptor => getFieldTypes(elem)
            case elem: BaseClassDescriptor => getFieldTypes(elem)
            case _                         => Seq()
          }
          listField +: elemFields
        }
        case CaseClassDescriptor(_, _, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getFieldTypes(f.desc) }
        case BaseClassDescriptor(_, _, getters, subTypes) => (getters flatMap { f => getFieldTypes(f.desc) }) ++ (subTypes flatMap getFieldTypes)
        case _ => Seq()
      }

      getFieldTypes(desc) toList match {
        case Nil => (Nil, Nil)
        case types =>
          val fields = types map { case (id, tpe) => mkVar(udtSerClassSym, "w" + id, Flags.PRIVATE, true, tpe) { _ => New(TypeTree(tpe), List(List())) } }
          (fields, types)
      }
    }

    private def mkMutableUdtInst(holderClassSym: Symbol, desc: CaseClassDescriptor): Tree = {
      val args = desc.getters map {
        case FieldAccessor(_, _, fTpe, _, _) => {
          mkDefault(fTpe.typeSymbol)
        }
      }

      val ctor = New(TypeTree(desc.tpe), List(args.toList))
      mkVar(holderClassSym, "mutableUdtInst" + desc.id, Flags.PRIVATE, true, desc.tpe) { _ => ctor }
    }

    private def mkReadObject(udtSerClassSym: Symbol, wrappers: List[(Int, Type)], mutableUdts: List[CaseClassDescriptor]): List[Tree] = {
      def mkUdtInit(desc: CaseClassDescriptor): Tree = {
        val args = desc.getters map {
          case FieldAccessor(_, _, fTpe, _, _) => {
            mkDefault(fTpe.typeSymbol)
          }
        }

        val ctor = New(TypeTree(desc.tpe), List(args.toList))
        Assign(Ident("mutableUdtInst" + desc.id), ctor)

      }

      val readObject = mkMethod(udtSerClassSym, "readObject", Flags.PRIVATE, List(("in", objectInputStreamClass.tpe)), definitions.UnitClass.tpe) { _ =>
        val default = Apply(Select(Ident("in"), "defaultReadObject"), Nil)
        val initWrappers = wrappers map { case (id, tpe) => Assign(Ident("w" + id), New(TypeTree(tpe), List(List()))) }
        val initMutables = mutableUdts map { mkUdtInit(_) }
        val ret = mkUnit
        Block(((default +: (initWrappers ++ initMutables)) :+ ret): _*)
      }
      List(readObject)
    }

    private def mkGetFieldIndex(udtSerClassSym: Symbol, desc: UDTDescriptor): Tree = {

      val env = GenEnvironment(udtSerClassSym, NoSymbol, Map(), "flat" + desc.id, false, true, true, true)

      def mkCases(desc: UDTDescriptor, path: Seq[String]): Seq[(Seq[String], Tree)] = desc match {

        case PrimitiveDescriptor(id, _, _, _)            => Seq((path, mkList(List(env.mkSelectIdx(id)))))
        case BoxedPrimitiveDescriptor(id, _, _, _, _, _) => Seq((path, mkList(List(env.mkSelectIdx(id)))))

        case BaseClassDescriptor(_, _, Seq(tag, getters @ _*), _) => {
          val tagCase = Seq((path :+ "getClass", mkList(List(env.mkSelectIdx(tag.desc.id)))))
          val fieldCases = getters flatMap { f => mkCases(f.desc, path :+ f.getter.name.toString) }
          tagCase ++ fieldCases
        }

        case CaseClassDescriptor(_, _, _, _, _, getters) => {
          def fieldCases = getters flatMap { f => mkCases(f.desc, path :+ f.getter.name.toString) }
          val allFieldsCase = desc match {
            case _ if desc.isPrimitiveProduct => {
              val nonRest = fieldCases filter { case (p, _) => p.size == path.size + 1 } map { _._2 }
              Seq((path, nonRest.reduceLeft((z, f) => Apply(Select(z, "$plus$plus"), List(f)))))
            }
            case _ => Seq()
          }
          allFieldsCase ++ fieldCases
        }

        case OpaqueDescriptor(id, _, _) => {
          val endPath = (path, Apply(Select(env.mkSelectSerializer(id), "getFieldIndex"), List(mkSeq(Nil))))
          val restPath = (path :+ null, Apply(Select(env.mkSelectSerializer(id), "getFieldIndex"), List(Ident("rest"))))
          Seq(endPath, restPath)
        }

        case _ => Seq()
      }

      def mkPat(unapplyDummy: Symbol, path: Seq[String]): Tree = {

        val seqUnapply = TypeApply(mkSelect("scala", "collection", "Seq", "unapplySeq"), List(TypeTree(stringTpe)))
        val fun = Apply(seqUnapply, List(mkIdent(unapplyDummy)))

        val args = path map {
          case null => Bind("rest", Star(Ident("_")))
          case s    => Literal(s)
        }

        UnApply(fun, args.toList)
      }

      mkMethod(udtSerClassSym, "getFieldIndex", Flags.OVERRIDE | Flags.FINAL, List(("selection", mkSeqOf(stringTpe))), mkListOf(intTpe)) { methodSym =>

        val unapplyDummy = methodSym.newValue(curPos, nme.SELECTOR_DUMMY) setFlag Flags.SYNTHETIC setInfo mkSeqOf(stringTpe)

        val cases = mkCases(desc, Seq()) map { case (path, idxs) => CaseDef(mkPat(unapplyDummy, path), EmptyTree, idxs) }
        val errCase = CaseDef(Ident("_"), EmptyTree, Apply(Select(This(udtSerClassSym), "invalidSelection"), List(Ident("selection"))))
        Match(Ident("selection"), (cases :+ errCase).toList)
      }
    }

    protected case class GenEnvironment(udtSerClassSym: Symbol, methodSym: Symbol, listImpls: Map[Int, Type], idxPrefix: String, reentrant: Boolean, allowRecycling: Boolean, chkIndex: Boolean, chkNull: Boolean) {

      def mkChkNotNull(source: Tree, tpe: Type): Tree = if (!tpe.isNotNull && chkNull) Apply(Select(source, "$bang$eq"), List(mkNull)) else EmptyTree
      def mkChkIdx(fieldId: Int): Tree = if (chkIndex) Apply(Select(mkSelectIdx(fieldId), "$greater$eq"), List(mkZero)) else EmptyTree

      def mkSelectIdx(fieldId: Int): Tree = Select(This(udtSerClassSym), idxPrefix + "Idx" + fieldId)
      def mkSelectSerializer(fieldId: Int): Tree = Select(This(udtSerClassSym), idxPrefix + "Ser" + fieldId)
      def mkSelectWrapper(fieldId: Int): Tree = Select(This(udtSerClassSym), "w" + fieldId)
      def mkSelectMutableUdtInst(udtId: Int): Tree = Select(This(udtSerClassSym), "mutableUdtInst" + udtId)

      def mkCallSetMutableField(udtId: Int, setter: Symbol, source: Tree): Tree = Apply(Select(mkSelectMutableUdtInst(udtId), setter), List(source))
      def mkCallSerialize(refId: Int, source: Tree, target: Tree): Tree = Apply(Select(This(udtSerClassSym), "serialize" + refId), List(source, target))
      def mkCallDeserialize(refId: Int, source: Tree): Tree = Apply(Select(This(udtSerClassSym), "deserialize" + refId), List(source))

      def mkSetField(fieldId: Int, record: Tree): Tree = mkSetField(fieldId, record, mkSelectWrapper(fieldId))
      def mkSetField(fieldId: Int, record: Tree, wrapper: Tree): Tree = Apply(Select(record, "setField"), List(mkSelectIdx(fieldId), wrapper))
      
      def mkSetValue(fieldId: Int, value: Tree): Tree = mkSetValue(mkSelectWrapper(fieldId), value)
      def mkSetValue(wrapper: Tree, value: Tree): Tree = Apply(Select(wrapper, "setValue"), List(value))

      def mkGetField(fieldId: Int, record: Tree): Tree = mkGetField(fieldId, record, mkSelectWrapper(fieldId))
      def mkGetField(fieldId: Int, record: Tree, wrapper: Tree): Tree = Apply(Select(record, "getField"), List(mkSelectIdx(fieldId), wrapper))
      
      def mkGetFieldValue(fieldId: Int, record: Tree): Tree = mkGetFieldValue(fieldId, record, mkSelectWrapper(fieldId))
      def mkGetFieldValue(fieldId: Int, record: Tree, wrapper: Tree): Tree = Apply(Select(mkGetField(fieldId, record, wrapper), "getValue"), List())
      
      def mkGetValue(fieldId: Int): Tree = mkGetValue(mkSelectWrapper(fieldId))
      def mkGetValue(wrapper: Tree): Tree = Apply(Select(wrapper, "getValue"), List())

      def mkNotIsNull(fieldId: Int, record: Tree): Tree = Select(Apply(Select(record, "isNull"), List(mkSelectIdx(fieldId))), "unary_$bang")
    }
  }
}