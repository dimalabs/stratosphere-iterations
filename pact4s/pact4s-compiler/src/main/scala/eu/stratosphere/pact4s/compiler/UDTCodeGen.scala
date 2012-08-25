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

package eu.stratosphere.pact4s.compiler

import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.symtab.Flags._
import scala.tools.nsc.transform.Transform

import eu.stratosphere.pact4s.compiler.util._

trait UDTCodeGeneration { this: Pact4sGlobal =>

  import global._
  import defs._

  trait UDTCodeGenerator extends PluginComponent with Transform {

    override val global: ThisGlobal = UDTCodeGeneration.this.global
    override val phaseName = "Pact4s.UDTCodeGen"

    val genSites: collection.Map[CompilationUnit, MutableMultiMap[Tree, UDTDescriptor]]

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) {

      logger.messageTag = "UDTCode"

      private val genSites = UDTCodeGenerator.this.genSites(unit)
      private val unitRoot = new EagerAutoSwitch[Tree] { override def guard = unit.toString.contains("Test.scala") }

      private val genHelper = new TreeGenHelper(unit)
      import genHelper._

      override def transform(tree: Tree): Tree = {

        logger.currentPosition = tree.pos

        visually(unitRoot) {

          tree match {

            // Generate UDT classes and inject them into the AST
            case Block(stats, ret) if genSites(tree).nonEmpty => {

              atOwner(tree, currentOwner) {
                super.transform {

                  verbosely[Tree] { tree => "GenSite Block[" + tree.pos.line + ":" + tree.pos.column + "] defines: " + localTyper.context.implicitss.flatten.filter(_.sym.owner == currentOwner).map(m => m.name.toString + ": " + m.tpe.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ") } {

                    val udtInstances = genSites(tree).toList flatMap { mkUdtInst(currentOwner, _) }
                    val newBlock @ Block(newStats, _) = localTyper.typed { treeCopy.Block(tree, udtInstances ++ stats, ret) }

                    // Blocks are naked (no symbol), so they don't maintain a scope.
                    // Enter the new implicits directly into the typer's scope instead.
                    localTyper.context.scope = localTyper.context.scope.cloneScope
                    newStats filter { stat => stat.hasSymbol && stat.symbol.isImplicit && stat.symbol.owner == currentOwner } map { _.symbol } foreach { localTyper.context.scope.enter }

                    newBlock
                  }
                }
              }
            }

            // Generate UDT classes and inject them into the AST
            case ClassDef(mods, name, tparams, template @ Template(parents, self, body)) if genSites(tree).nonEmpty => {

              super.transform {

                verbosely[Tree] { tree => "GenSite " + tree.symbol + " defines: " + tree.symbol.tpe.members.filter(_.isImplicit).map(m => m.name.toString + ": " + m.tpe.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ") } {
                  val udtInstances = genSites(tree).toList flatMap { mkUdtInst(tree.symbol, _) }
                  localTyper.typed { treeCopy.ClassDef(tree, mods, name, tparams, treeCopy.Template(template, parents, self, udtInstances ::: body)) }
                }
              }
            }

            // Rerun implicit inference at call sites bound to unanalyzedUdt
            case TypeApply(s: Select, List(t)) if s.symbol == unanalyzedUdt => {

              super.transform {

                safely(tree) { e => "Error applying UDT[" + t.tpe + "]: " + getMsgAndStackLine(e) } {

                  val udtInst = analyzer.inferImplicit(tree, mkUdtOf(t.tpe), true, false, localTyper.context)

                  udtInst.tree match {
                    case t if t.isEmpty || t.symbol == unanalyzedUdt => {
                      val availUdts = localTyper.context.implicitss.flatten.map(m => m.name.toString + ": " + m.tpe.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ")
                      val implicitCount = localTyper.context.implicitss.flatten.length
                      log(Error) { "Failed to apply " + mkUdtOf(t.tpe) + ". Total Implicits: " + implicitCount + ". Available UDTs: " + availUdts }
                      tree
                    }
                    case udtInst => {
                      log(Debug) { "Applied " + udtInst.symbol.fullName + ": " + udtInst.tpe }
                      localTyper.typed { udtInst }
                    }
                  }
                }
              }
            }

            case _ => super.transform(tree)
          }
        }
      }

      private def mkUdtInst(owner: Symbol, desc: UDTDescriptor): List[Tree] = {

        safely(Nil: List[Tree]) { e => "Error generating UDT[" + desc.tpe + "]: " + getMsgAndStackLine(e) } {
          verbosely[List[Tree]] { case l => { val List(_, t) = l; "Generated " + t.symbol.fullName + "[" + desc.tpe + "] @ " + owner + " : " + t } } {

            val privateFlag = if (owner.isClass) PRIVATE else 0

            val List(valDef, defDef) = mkVarAndLazyGetter(owner, unit.freshTermName("udtInst(") + ")", privateFlag | IMPLICIT, mkUdtOf(desc.tpe)) { defSym =>

              val udtClassDef = mkUdtClass(defSym, desc)
              val udtInst = New(TypeTree(udtClassDef.symbol.tpe), List(List()))

              Block(udtClassDef, udtInst)
            }

            if (owner.isClass) {
              owner.info.decls enter valDef.symbol
              owner.info.decls enter defDef.symbol
            }

            // Why is the UnCurry phase unhappy if we don't run the typer here?
            // We're already running it for the enclosing ClassDef...
            try {
              List(localTyper.typed { valDef }, localTyper.typed { defDef })
            } catch {
              case e => { Debug.browse(Block(valDef, defDef)); throw e }
            }
          }
        }
      }

      private def mkUdtClass(owner: Symbol, desc: UDTDescriptor): Tree = {

        mkClass(owner, unit.freshTypeName("UDTImpl"), FINAL, List(definitions.ObjectClass.tpe, mkUdtOf(desc.tpe), definitions.SerializableClass.tpe)) { classSym =>
          mkFieldTypes(classSym, desc) :+ mkCreateSerializer(classSym, desc)
        }
      }

      private def mkFieldTypes(udtClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

        val pactValueTpe = mkExistentialType(udtClassSym, definitions.ClassClass.tpe, pactValueClass.tpe)
        val pactListTpe = mkExistentialType(udtClassSym, pactListBaseClass.tpe, pactValueClass.tpe)

        mkValAndGetter(udtClassSym, "fieldTypes", OVERRIDE | FINAL, definitions.arrayType(pactValueTpe)) { _ =>

          def getFieldTypes(desc: UDTDescriptor): Seq[Tree] = desc match {
            case PrimitiveDescriptor(_, _, _, wrapper)            => Seq(mkClassOf(wrapper.tpe))
            case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, _) => Seq(mkClassOf(wrapper.tpe))
            case ListDescriptor(_, _, _, _, _, elem)              => Seq(mkClassOf(pactListTpe))
            // Flatten product types
            case CaseClassDescriptor(_, _, _, _, getters)         => getters filterNot { _.isBaseField } flatMap { f => getFieldTypes(f.desc) }
            // Tag and flatten summation types
            // TODO (Joe): Rather than laying subclasses out sequentially, just 
            //             reserve enough fields for the largest subclass.
            //             This is tricky because subclasses can contain opaque
            //             descriptors, so we don't know how many fields we
            //             need until runtime.
            case BaseClassDescriptor(_, _, getters, subTypes)     => (getters flatMap { f => getFieldTypes(f.desc) }) ++ (subTypes flatMap getFieldTypes)
            case OpaqueDescriptor(_, _, ref)                      => Seq(Select(ref, "fieldTypes"))
            // Box inner instances of recursive types
            case RecursiveDescriptor(_, _, _)                     => Seq(mkClassOf(pactRecordClass.tpe))
          }

          val fieldSets = getFieldTypes(desc).foldRight(Seq[Tree]()) { (f, z) =>
            (f, z) match {
              case (_: Select, _)                => f +: z
              case (_, ArrayValue(tpe, fs) :: r) => ArrayValue(tpe, f +: fs) +: r
              case _                             => ArrayValue(TypeTree(pactValueTpe), List(f)) +: z
            }
          }

          fieldSets match {
            case Seq(a) => a
            case as     => Apply(TypeApply(Select(Select(Ident("scala"), "Array"), "concat"), List(TypeTree(pactValueTpe))), as.toList)
          }
        }
      }

      private def mkCreateSerializer(udtClassSym: Symbol, desc: UDTDescriptor): Tree = {

        mkMethod(udtClassSym, "createSerializer", OVERRIDE | FINAL, List(("indexMap", intArrayTpe)), mkUdtSerializerOf(desc.tpe)) { methodSym =>
          val udtSer = mkUdtSerializerClass(methodSym, desc)
          Block(udtSer, New(TypeTree(udtSer.symbol.tpe), List(List())))
        }
      }

      private def mkUdtSerializerClass(owner: Symbol, desc: UDTDescriptor): Tree = {

        mkClass(owner, unit.freshTypeName("UDTSerializerImpl"), FINAL, List(mkUdtSerializerOf(desc.tpe), definitions.SerializableClass.tpe)) { classSym =>

          val (listImpls, listImplTypes) = mkListImplClasses(classSym, desc)

          val indexMapIter = Select(Apply(Select(Select(Ident("scala"), "Predef"), "intArrayOps"), List(Ident("indexMap"))), "iterator")
          val (fields1, inits1) = mkIndexes(classSym, desc.id, getIndexFields(desc).toList, false, indexMapIter)
          val (fields2, inits2) = mkBoxedIndexes(classSym, desc)

          val fields = fields1 ++ fields2
          val init = inits1 ++ inits2 match {
            case Nil   => Nil
            case inits => List(mkMethod(classSym, "init", OVERRIDE | FINAL, List(), definitions.UnitClass.tpe) { _ => Block(inits: _*) })
          }

          listImpls ++ fields ++ mkPactWrappers(classSym, desc, listImplTypes) ++ init ++ mkSerialize(classSym, desc, listImplTypes) ++ mkDeserialize(classSym, desc)
        }
      }

      private def mkListImplClasses(udtSerClassSym: Symbol, desc: UDTDescriptor): (List[Tree], Map[Int, Type]) = {

        def mkListImplClass(elemTpe: Type): Tree = mkClass(udtSerClassSym, unit.freshTypeName("PactListImpl"), FINAL, List(mkPactListOf(elemTpe))) { _ => Nil }

        def getListTypes(desc: UDTDescriptor): Seq[(Int, Int, Type)] = desc match {
          case ListDescriptor(id, _, _, _, _, elem: ListDescriptor) => {
            val impls @ Seq((_, depth, primTpe), _*) = getListTypes(elem)
            (id, depth + 1, primTpe) +: impls
          }
          case ListDescriptor(id, _, _, _, _, elem: PrimitiveDescriptor) => Seq((id, 1, elem.wrapper.tpe))
          case ListDescriptor(id, _, _, _, _, elem: BoxedPrimitiveDescriptor) => Seq((id, 1, elem.wrapper.tpe))
          case ListDescriptor(id, _, _, _, _, elem) => (id, 1, pactRecordClass.tpe) +: getListTypes(elem)
          case BaseClassDescriptor(_, _, getters, subTypes) => (getters flatMap { f => getListTypes(f.desc) }) ++ (subTypes flatMap getListTypes)
          case CaseClassDescriptor(_, _, _, _, getters) => getters flatMap { f => getListTypes(f.desc) }
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

      def getIndexFields(desc: UDTDescriptor): Seq[UDTDescriptor] = desc match {
        case CaseClassDescriptor(_, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getIndexFields(f.desc) }
        case BaseClassDescriptor(id, _, getters, subTypes) => (getters flatMap { f => getIndexFields(f.desc) }) ++ (subTypes flatMap getIndexFields)
        case _ => Seq(desc)
      }

      private def mkIndexes(udtSerClassSym: Symbol, descId: Int, descFields: List[UDTDescriptor], boxed: Boolean, indexMapIter: Tree): (List[Tree], List[Tree]) = {

        val prefix = (if (boxed) "boxed" else "flat") + descId
        val iterName = prefix + "Iter"
        val iter = mkVal(udtSerClassSym, iterName, PRIVATE, true, mkIteratorOf(definitions.IntClass.tpe)) { _ => indexMapIter }

        val fieldsAndInits = descFields map {

          case OpaqueDescriptor(id, tpe, ref) => {
            val take = Apply(Select(Select(This(udtSerClassSym), iterName), "take"), List(Select(ref, "numFields")))
            val arr = Apply(TypeApply(Select(take, "toArray"), List(TypeTree(definitions.IntClass.tpe))), List(Select(Select(Ident("reflect"), "Manifest"), "Int")))
            val idxField = mkVal(udtSerClassSym, prefix + "Idx" + id, PRIVATE, true, intArrayTpe) { _ => arr }

            val serField = mkVar(udtSerClassSym, prefix + "Ser" + id, PRIVATE, false, mkUdtSerializerOf(tpe)) { _ => Literal(Constant(null)) }

            val serInst = Apply(Select(ref, "getSerializer"), List(Select(This(udtSerClassSym), prefix + "Idx" + id)))
            val serInit = Assign(Select(This(udtSerClassSym), prefix + "Ser" + id), serInst)

            (List(idxField, serField), List(serInit: Tree))
          }

          case d => {
            val next = Apply(Select(Select(This(udtSerClassSym), iterName), "next"), Nil)
            val idxField = mkVal(udtSerClassSym, prefix + "Idx" + d.id, PRIVATE, false, definitions.IntClass.tpe) { _ => next }

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
          case CaseClassDescriptor(_, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getBoxedDescriptors(f.desc) }
          case BaseClassDescriptor(id, _, getters, subTypes) => (getters flatMap { f => getBoxedDescriptors(f.desc) }) ++ (subTypes flatMap getBoxedDescriptors)
          case RecursiveDescriptor(_, _, refId) => desc.findById(refId).toSeq
          case _ => Seq()
        }

        val fieldsAndInits = getBoxedDescriptors(desc).distinct.toList flatMap { d =>
          getIndexFields(d).toList match {
            case Nil => None
            case fields => {
              val widths = fields map {
                case OpaqueDescriptor(_, _, ref) => Select(ref, "numFields")
                case _                           => Literal(1)
              }
              val sum = widths.reduce { (s, i) => Apply(Select(s, "$plus"), List(i)) }
              val range = Apply(Select(Ident("scala"), "Range"), List(Literal(0), sum))
              Some(mkIndexes(udtSerClassSym, d.id, fields, true, Select(range, "iterator")))
            }
          }
        }

        val (fields, inits) = fieldsAndInits.unzip
        (fields.flatten, inits.flatten)
      }

      def getInnermostListElem(desc: ListDescriptor): UDTDescriptor = desc.elem match {
        case elem: ListDescriptor => getInnermostListElem(elem)
        case elem                 => elem
      }

      private def mkPactWrappers(udtSerClassSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

        def getFieldTypes(desc: UDTDescriptor): Seq[(Int, Type)] = desc match {
          case PrimitiveDescriptor(id, _, _, wrapper)            => Seq((id, wrapper.tpe))
          case BoxedPrimitiveDescriptor(id, _, _, wrapper, _, _) => Seq((id, wrapper.tpe))
          case d @ ListDescriptor(id, _, _, _, _, elem) => {
            val listField = (id, listImpls(id))
            val elemFields = getInnermostListElem(d) match {
              case elem: CaseClassDescriptor => getFieldTypes(elem)
              case elem: BaseClassDescriptor => getFieldTypes(elem)
              case _                         => Seq()
            }
            listField +: elemFields
          }
          case CaseClassDescriptor(_, _, _, _, getters) => getters filterNot { _.isBaseField } flatMap { f => getFieldTypes(f.desc) }
          case BaseClassDescriptor(_, _, getters, subTypes) => (getters flatMap { f => getFieldTypes(f.desc) }) ++ (subTypes flatMap getFieldTypes)
          case _ => Seq()
        }

        getFieldTypes(desc) toList match {
          case Nil => Nil
          case types =>

            val fields = types map { case (id, tpe) => mkVar(udtSerClassSym, "w" + id, PRIVATE, true, tpe) { _ => New(TypeTree(tpe), List(List())) } }

            val readObject = mkMethod(udtSerClassSym, "readObject", PRIVATE, List(("in", objectInputStreamClass.tpe)), definitions.UnitClass.tpe) { _ =>
              val first = Apply(Select(Ident("in"), "defaultReadObject"), Nil)
              val rest = types map { case (id, tpe) => Assign(Ident("w" + id), New(TypeTree(tpe), List(List()))) }
              val ret = Literal(())
              Block(((first +: rest) :+ ret): _*)
            }

            fields :+ readObject
        }
      }

      private def mkSerialize(udtSerClassSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

        val root = mkMethod(udtSerClassSym, "serialize", OVERRIDE | FINAL, List(("item", desc.tpe), ("record", pactRecordClass.tpe)), definitions.UnitClass.tpe) { methodSym =>
          Block(genSerialize(udtSerClassSym, methodSym, desc, listImpls, "flat" + desc.id, false, Ident("item"), Ident("record"), true, true) toList, Literal(()))
        }

        val aux = (desc.findByType[RecursiveDescriptor].toList flatMap { rd => desc.findById(rd.refId) } distinct) map { desc =>
          mkMethod(udtSerClassSym, "serialize" + desc.id, PRIVATE | FINAL, List(("item", desc.tpe), ("record", pactRecordClass.tpe)), definitions.UnitClass.tpe) { methodSym =>
            Block(genSerialize(udtSerClassSym, methodSym, desc, listImpls, "boxed" + desc.id, true, Ident("item"), Ident("record"), false, false) toList, Literal(()))
          }
        }

        root +: aux
      }

      def genSerialize(udtSerClassSym: Symbol, methodSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type], idxPrefix: String, reentrant: Boolean, source: Tree, target: Tree, chkIndex: Boolean, chkNull: Boolean): Seq[Tree] = {

        def mkChkNotNull(source: Tree, tpe: Type, chkNull: Boolean): Tree = if (!tpe.isNotNull && chkNull) Apply(Select(source, "$bang$eq"), List(Literal(Constant(null)))) else EmptyTree
        def mkChkIdx(id: Int, chkIndex: Boolean): Tree = if (chkIndex) Apply(Select(Select(This(udtSerClassSym), idxPrefix + "Idx" + id), "$greater$eq"), List(Literal(0))) else EmptyTree
        def mkSetField(id: Int, source: Tree, target: Tree): Tree = Apply(Select(target, "setField"), List(Select(This(udtSerClassSym), idxPrefix + "Idx" + id), source))

        def genList(desc: ListDescriptor, source: Tree, target: Tree): Seq[Tree] = {

          val it = mkVal(methodSym, "it", 0, false, mkIteratorOf(desc.elem.tpe)) { _ => desc.iter(source) }

          val loop = mkWhile(Select(Ident(it.symbol), "hasNext")) {

            val item = mkVal(methodSym, "item", 0, false, desc.elem.tpe) { _ => Select(Ident(it.symbol), "next") }

            val (stats, value) = desc.elem match {

              case PrimitiveDescriptor(_, _, _, wrapper)                => (Seq(), New(TypeTree(wrapper.tpe), List(List(Ident(item.symbol)))))

              case BoxedPrimitiveDescriptor(_, _, _, wrapper, _, unbox) => (Seq(), New(TypeTree(wrapper.tpe), List(List(unbox(Ident(item.symbol))))))

              case elem @ ListDescriptor(id, _, _, _, _, innerElem) => {
                val listTpe = listImpls(id)
                val list = mkVal(methodSym, "list" + id, 0, false, listTpe) { _ => New(TypeTree(listTpe), List(List())) }
                val body = genList(elem, Ident(item.symbol), Ident(list.symbol))
                (list +: body, Ident(list.symbol))
              }

              case RecursiveDescriptor(id, tpe, refId) => {
                val rec = mkVal(methodSym, "record" + id, 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
                val ser = Apply(Select(This(udtSerClassSym), "serialize" + refId), List(Ident(item.symbol), Ident(rec.symbol)))
                val updRec = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())

                (Seq(rec, ser, updRec), Ident(rec.symbol))
              }

              case elem => {
                val rec = mkVal(methodSym, "record", 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
                val ser = genSerialize(udtSerClassSym, methodSym, elem, listImpls, "boxed" + elem.id, reentrant, Ident(item.symbol), Ident(rec.symbol), false, false)
                val upd = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())
                ((rec +: ser) :+ upd, Ident(rec.symbol))
              }
            }

            val chk = mkChkNotNull(Ident(item.symbol), desc.elem.tpe, true)
            val add = Apply(Select(target, "add"), List(value))
            val addNull = Apply(Select(target, "add"), List(Literal(Constant(null))))
            val body = item +: mkIf(chk, stats :+ add, Seq(addNull))

            Block(body: _*)
          }

          Seq(it, loop)
        }

        desc match {

          case PrimitiveDescriptor(id, _, _, _) => {
            val chk = mkChkIdx(id, chkIndex)
            val ser = Apply(Select(Select(This(udtSerClassSym), "w" + id), "setValue"), List(source))
            val set = mkSetField(id, Select(This(udtSerClassSym), "w" + id), target)

            mkIf(chk, ser, set)
          }

          case BoxedPrimitiveDescriptor(id, tpe, _, _, _, unbox) => {
            val chk = mkAnd(mkChkIdx(id, chkIndex), mkChkNotNull(source, tpe, chkNull))
            val ser = Apply(Select(Select(This(udtSerClassSym), "w" + id), "setValue"), List(unbox(source)))
            val set = mkSetField(id, Select(This(udtSerClassSym), "w" + id), target)

            mkIf(chk, ser, set)
          }

          case d @ ListDescriptor(id, _, _, _, _, elem) => {
            val chk = mkAnd(mkChkIdx(d.id, chkIndex), mkChkNotNull(source, d.tpe, chkNull))

            val upd = getInnermostListElem(d) match {
              case _: RecursiveDescriptor => Some(Apply(Select(target, "updateBinaryRepresenation"), List()))
              case _                      => None
            }

            val stats = reentrant match {

              // TODO (Joe): Optimize this to reuse list wrappers when it's safe to do so
              case true => {
                val listTpe = listImpls(id)
                val list = mkVal(methodSym, "list" + id, 0, false, listTpe) { _ => New(TypeTree(listTpe), List(List())) }
                val body = genList(d, source, Ident(list.symbol))
                val set = mkSetField(id, Ident(list.symbol), target)
                (list +: body) :+ set
              }

              case false => {
                val clear = Apply(Select(Select(This(udtSerClassSym), "w" + id), "clear"), List())
                val body = genList(d, source, Select(This(udtSerClassSym), "w" + id))
                val set = mkSetField(id, Select(This(udtSerClassSym), "w" + id), target)
                (clear +: body) :+ set
              }
            }

            mkIf(chk, (upd.toSeq ++ stats): _*)
          }

          case CaseClassDescriptor(_, tpe, _, _, getters) => {
            val chk = mkChkNotNull(source, tpe, chkNull)
            val stats = getters filterNot { _.isBaseField } flatMap { case FieldAccessor(sym, _, _, desc) => genSerialize(udtSerClassSym, methodSym, desc, listImpls, idxPrefix, reentrant, Select(source, sym), target, chkIndex, true) }

            mkIf(chk, stats: _*)
          }

          case BaseClassDescriptor(id, tpe, Seq(tagField, baseFields @ _*), subTypes) => {
            val chk = mkChkNotNull(source, tpe, chkNull)
            val fields = baseFields flatMap { f => genSerialize(udtSerClassSym, methodSym, f.desc, listImpls, idxPrefix, reentrant, Select(source, f.sym), target, chkIndex, true) }
            val cases = subTypes.zipWithIndex.toList map {
              case (dSubType, i) => {
                val tag = genSerialize(udtSerClassSym, methodSym, tagField.desc, listImpls, idxPrefix, reentrant, Literal(i), target, chkIndex, false)
                val code = genSerialize(udtSerClassSym, methodSym, dSubType, listImpls, idxPrefix, reentrant, Ident("inst"), target, chkIndex, false)
                val body = (tag ++ code) :+ Literal(())

                val pat = Bind("inst", Typed(Ident("_"), TypeTree(dSubType.tpe)))
                CaseDef(pat, EmptyTree, Block(body: _*))
              }
            }

            mkIf(chk, (fields :+ Match(source, cases)): _*)
          }

          case OpaqueDescriptor(id, tpe, _) => {
            val ser = Apply(Select(Select(This(udtSerClassSym), idxPrefix + "Ser" + id), "serialize"), List(source, target))
            mkIf(mkChkNotNull(source, tpe, chkNull), ser)
          }

          case RecursiveDescriptor(id, tpe, refId) => {
            // Important: recursive types introduce re-entrant calls to serialize()

            val chk = mkAnd(mkChkIdx(id, chkIndex), mkChkNotNull(source, tpe, chkNull))

            // Persist the outer record prior to recursing, since the call
            // is going to reuse all the PactPrimitive wrappers that were 
            // needed *before* the recursion.
            val updTgt = Apply(Select(target, "updateBinaryRepresenation"), List())

            val rec = mkVal(methodSym, "record" + id, 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
            val ser = Apply(Select(This(udtSerClassSym), "serialize" + refId), List(source, Ident(rec.symbol)))

            // Persist the new inner record after recursing, since the
            // current call is going to reuse all the PactPrimitive
            // wrappers that are needed *after* the recursion.
            val updRec = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())

            val set = mkSetField(id, Ident(rec.symbol), target)

            mkIf(chk, updTgt, rec, ser, updRec, set)
          }
        }
      }

      private def mkDeserialize(udtSerClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

        val root = mkMethod(udtSerClassSym, "deserialize", OVERRIDE | FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { _ =>
          desc match {
            case PrimitiveDescriptor(_, _, default, _) => default
            case BoxedPrimitiveDescriptor(_, _, _, _, _, _) => Literal(Constant(null))
            case _ => Literal(Constant(null))
          }
        }

        List(root)
      }
    }
  }
}

