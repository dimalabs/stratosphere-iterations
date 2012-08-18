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

  trait UDTCodeGenerator extends PluginComponent with Transform {

    override val global: ThisGlobal = UDTCodeGeneration.this.global
    override val phaseName = "Pact4s.UDTCodeGen"

    val genSites: collection.Map[CompilationUnit, MutableMultiMap[Tree, UDTDescriptor]]

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) {

      UDTCodeGeneration.this.messageTag = "UDTCode"
      private val genSites = UDTCodeGenerator.this.genSites(unit)
      private val unitRoot = new EagerAutoSwitch[Tree] { override def guard = unit.toString.contains("Test.scala") }

      override def transform(tree: Tree): Tree = {

        currentPosition = tree.pos

        visually(unitRoot) {

          tree match {

            // HACK: Blocks are naked (no symbol), so there's no scope in which to insert new implicits. 
            //       Wrap the block in an anonymous class, process the tree, then unpack the result.
            case block: Block if genSites(tree).nonEmpty => {

              val (wrappedBlock, wrapper) = mkBlockWrapper(currentOwner, block)
              val result = super.transform(localTyper.typed { wrappedBlock })

              detectWrapperArtifacts(wrapper) {
                localTyper.typed { unwrapBlock(currentOwner, wrapper, result) }
              }
            }

            // Generate UDT classes and inject them into the AST
            case ClassDef(mods, name, tparams, template @ Template(parents, self, body)) if genSites(tree).nonEmpty => {

              super.transform {

                val udtInstances = genSites(tree).toList flatMap { mkUdtInst(tree.symbol, _) }
                //log(Debug) { "GenSite " + tree.symbol + " defines:   " + tree.symbol.tpe.members.filter(_.isImplicit).map(m => m.name.toString + ": " + m.tpe.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ") }

                verbosely[Tree] { tree => "GenSite " + tree.symbol + " defines: " + tree.symbol.tpe.members.filter(_.isImplicit).map(m => m.name.toString + ": " + m.tpe.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ") } {
                  localTyper.typed { treeCopy.ClassDef(tree, mods, name, tparams, treeCopy.Template(template, parents, self, udtInstances ::: body)) }
                }
              }
            }

            // Rerun implicit inference at call sites bound to unanalyzedUdt
            case TypeApply(s: Select, List(t)) if s.symbol == unanalyzedUdt => {

              super.transform {

                safely(tree) { e => "Error applying UDT[" + t.tpe + "]: " + e.getMessage() + " @ " + getRelevantStackLine(e) } {

                  val udtTpe = appliedType(udtClass.tpe, List(t.tpe))
                  val udtInst = analyzer.inferImplicit(tree, udtTpe, true, false, localTyper.context)

                  udtInst.tree match {
                    case t if t.isEmpty || t.symbol == unanalyzedUdt => {
                      log(Error) { "Failed to apply " + udtTpe + ". Available UDTs: " + localTyper.context.implicitss.flatten.map(m => m.name.toString + ": " + m.tpe.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ") }
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

      private def mkBlockWrapper(owner: Symbol, site: Block): (Tree, Symbol) = {

        safely[(Tree, Symbol)](site: Tree, NoSymbol) { e => "Error generating BlockWrapper in " + owner + ": " + e.getMessage() + " @ " + getRelevantStackLine(e) } {

          val wrapper = this.mkClass(owner, null, FINAL, List(definitions.ObjectClass.tpe)) { classSym =>

            List(mkMethod(classSym, "result", FINAL, Nil, site.expr.tpe) { _ =>

              applyTransformation(site) { tree =>
                if (tree.hasSymbol && tree.symbol.owner == owner)
                  tree.symbol.owner = classSym
                tree
              }
            })
          }

          val wrappedBlock = Select(Block(wrapper, New(TypeTree(wrapper.symbol.tpe), List(List()))), "result")
          genSites(wrapper) ++= genSites(site)
          genSites.remove(site)

          (wrappedBlock, wrapper.symbol)
        }
      }

      private def unwrapBlock(owner: Symbol, wrapper: Symbol, tree: Tree): Tree = {

        safely(tree) { e => "Error unwrapping BlockWrapper in " + owner + ": " + e.getMessage() + " @ " + getRelevantStackLine(e) } {

          val Select(Block(List(cd: ClassDef), _), _) = tree
          val ClassDef(_, _, _, Template(_, _, body)) = cd
          val Some(DefDef(_, _, _, _, _, rhs: Block)) = body find { item => item.hasSymbol && item.symbol.name.toString == "result" }

          val newImplicits = body filter { m => m.hasSymbol && m.symbol.isImplicit }
          val newSyms = newImplicits map { _.symbol } toSet

          val rewiredRhs = applyTransformation(rhs) {
            _ match {
              case sel: Select if sel.hasSymbol && newSyms.contains(sel.symbol) => mkIdent(sel.symbol)
              case tree => {
                if (tree.hasSymbol && tree.symbol.owner == wrapper)
                  tree.symbol.owner = owner
                tree
              }
            }
          }

          val rewiredImplicits = newImplicits map { imp =>
            val sym = imp.symbol
            val ValDef(_, _, _, rhs) = imp

            sym.owner = owner
            sym.resetFlag(PRIVATE)

            ValDef(sym, rhs) setType sym.tpe
          }

          val Block(stats, expr) = rewiredRhs
          treeCopy.Block(rewiredRhs, rewiredImplicits ::: stats, expr)
        }
      }

      // Sanity check - make sure we've properly cleaned up after ourselves
      private def detectWrapperArtifacts(wrapper: Symbol)(tree: Tree): Tree = {

        val detected = new ManualSwitch[Tree]

        visually(detected) {
          applyTransformation(tree) { tree =>

            detected |= (tree.hasSymbol && tree.symbol.hasTransOwner(wrapper))

            if (tree.tpe == null) {
              log(Error) { "Unwrapped tree has no type [" + tree.shortClass + "]: " + tree }
            } else {
              detected |= (tree.tpe filter { tpe => tpe.typeSymbol.hasTransOwner(wrapper) || tpe.termSymbol.hasTransOwner(wrapper) }).nonEmpty
            }

            if (detected.state)
              log(Error) { "Wrapper artifact detected [" + tree.shortClass + "]: " + tree }

            tree
          }
        }
      }

      private def mkUdtInst(owner: Symbol, desc: UDTDescriptor): List[Tree] = {

        safely(Nil: List[Tree]) { e => "Error generating UDT[" + desc.tpe + "]: " + e.getMessage() + " @ " + getRelevantStackLine(e) } {
          verbosely[List[Tree]] { case l => { val List(_, t) = l; "Generated " + t.symbol.fullName + "[" + desc.tpe + "] @ " + owner + " : " + t } } {

            val udtTpe = appliedType(udtClass.tpe, List(desc.tpe))

            val List(valDef, defDef) = mkVarAndLazyGetter(owner, unit.freshTermName("udtInst(") + ")", PRIVATE | IMPLICIT, udtTpe) { defSym =>

              val udtClassDef = mkUdtClass(defSym, desc)
              val udtInst = New(TypeTree(udtClassDef.symbol.tpe), List(List()))

              Block(udtClassDef, udtInst)
            }

            owner.info.decls enter valDef.symbol
            owner.info.decls enter defDef.symbol

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

        mkClass(owner, "UDTImpl", FINAL, List(definitions.ObjectClass.tpe, appliedType(udtClass.tpe, List(desc.tpe)), definitions.SerializableClass.tpe)) { classSym =>
          mkFieldTypes(classSym, desc) :+ mkCreateSerializer(classSym, desc)
        }
      }

      private def mkFieldTypes(udtClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

        val elemTpe = {
          val exVar = udtClassSym.newAbstractType(newTypeName("_$1")) setInfo TypeBounds.upper(pactValueClass.tpe)
          ExistentialType(List(exVar), appliedType(definitions.ClassClass.tpe, List(TypeRef(NoPrefix, exVar, Nil))))
        }

        val valTpe = definitions.arrayType(elemTpe)

        mkValAndGetter(udtClassSym, "fieldTypes", OVERRIDE | FINAL, valTpe) { _ =>

          def getFieldTypes(desc: UDTDescriptor): Seq[Tree] = desc match {
            case OpaqueDescriptor(_, _, ref, false)                       => Seq(Select(ref, "fieldTypes"))
            // Box inner instances of recursive types
            case OpaqueDescriptor(_, _, ref, true)                        => Seq(gen.mkClassOf(pactRecordClass.tpe))
            case PrimitiveDescriptor(_, _, _, sym)                        => Seq(gen.mkClassOf(sym.tpe))
            case ListDescriptor(_, _, _, _, _, elem: PrimitiveDescriptor) => Seq(gen.mkClassOf(appliedType(pactListClass.tpe, List(elem.wrapperClass.tpe))))
            // Box non-primitive list elements
            case ListDescriptor(_, _, _, _, _, _)                         => Seq(gen.mkClassOf(appliedType(pactListClass.tpe, List(pactRecordClass.tpe))))
            // Flatten product types
            case CaseClassDescriptor(_, _, _, _, getters)                 => getters flatMap { getter => getFieldTypes(getter.descr) }
            // Tag and flatten summation types
            // TODO (Joe): Rather than laying subclasses out sequentially, just 
            //             reserve enough fields for the largest subclass.
            //             This is tricky because subclasses can contain opaque
            //             descriptors, so we don't know how many fields we
            //             need until runtime.
            // TODO (Joe): Merge abstract base fields implemented by subclasses
            //             so that they are visible to key selector functions.
            case BaseClassDescriptor(_, _, subTypes)                      => gen.mkClassOf(pactIntegerClass.tpe) +: (subTypes flatMap { subType => getFieldTypes(subType) })
          }

          val fieldSets = getFieldTypes(desc).foldRight(Seq[Tree]()) { (f, z) =>
            (f, z) match {
              case (_: Select, _)                => f +: z
              case (_, ArrayValue(tpe, fs) :: r) => ArrayValue(tpe, f +: fs) +: r
              case _                             => ArrayValue(TypeTree(elemTpe), List(f)) +: z
            }
          }

          fieldSets match {
            case Seq(a) => a
            case as     => Apply(TypeApply(Select(Select(Ident("scala"), "Array"), "concat"), List(TypeTree(elemTpe))), as.toList)
          }
        }
      }

      private def mkCreateSerializer(udtClassSym: Symbol, desc: UDTDescriptor): Tree = {

        val indexMapTpe = appliedType(definitions.ArrayClass.tpe, List(definitions.IntClass.tpe))
        val udtSerTpe = appliedType(udtSerializerClass.tpe, List(desc.tpe))

        mkMethod(udtClassSym, "createSerializer", OVERRIDE | FINAL, List(("indexMap", indexMapTpe)), udtSerTpe) { methodSym =>
          val udtSer = mkUdtSerializerClass(methodSym, desc)
          Block(udtSer, New(TypeTree(udtSer.symbol.tpe), List(List())))
        }
      }

      private def mkUdtSerializerClass(owner: Symbol, desc: UDTDescriptor): Tree = {

        mkClass(owner, "UDTSerializerImpl", FINAL, List(appliedType(udtSerializerClass.tpe, List(desc.tpe)), definitions.SerializableClass.tpe)) { classSym =>
          mkIndexes(classSym, desc) ++ mkInners(classSym, desc) ++ mkPactHolders(classSym, desc) ++ List(mkSerialize(classSym, desc), mkDeserialize(classSym, desc))
        }
      }

      private def mkIndexes(udtSerClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

        def getWidths(desc: UDTDescriptor): Seq[(Int, Tree)] = desc match {
          case OpaqueDescriptor(id, _, ref, false)      => Seq((id, Select(ref, "numFields")))
          case CaseClassDescriptor(_, _, _, _, getters) => getters flatMap { getter => getWidths(getter.descr) }
          case BaseClassDescriptor(id, _, subTypes)     => (id, Literal(1)) +: (subTypes flatMap { subType => getWidths(subType) })
          case _                                        => Seq((desc.id, Literal(1)))
        }

        val intTpe = definitions.IntClass.tpe
        val arrTpe = appliedType(definitions.ArrayClass.tpe, List(intTpe))
        val iterTpe = appliedType(definitions.IteratorClass.tpe, List(intTpe))

        val iter = mkVal(udtSerClassSym, "idxIter", PRIVATE, true, iterTpe) { _ =>
          Select(Apply(Select(Select(Ident("scala"), "Predef"), "intArrayOps"), List(Ident("indexMap"))), "iterator")
        }

        val indexes = getWidths(desc) map {
          case (id, _: Literal) => mkVal(udtSerClassSym, "idx" + id, PRIVATE, false, intTpe) { _ => Apply(Select(Select(This(udtSerClassSym), "idxIter"), "next"), Nil) }
          case (id, w)          => mkVal(udtSerClassSym, "idx" + id, PRIVATE, false, arrTpe) { _ => Apply(TypeApply(Select(Apply(Select(Select(This(udtSerClassSym), "idxIter"), "take"), List(w)), "toArray"), List(TypeTree(intTpe))), List(Select(Select(Ident("reflect"), "Manifest"), "Int"))) }
        }

        (iter +: indexes) toList
      }

      private def mkInners(udtSerClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

        def getInnerTypes(desc: UDTDescriptor, box: Boolean): Seq[(Boolean, OpaqueDescriptor)] = desc match {
          case d @ OpaqueDescriptor(_, _, _, dBox)      => Seq((box || dBox, d))
          case ListDescriptor(_, _, _, _, _, elem)      => getInnerTypes(elem, true)
          case CaseClassDescriptor(_, _, _, _, getters) => getters flatMap { getter => getInnerTypes(getter.descr, box) }
          case BaseClassDescriptor(_, _, subTypes)      => subTypes flatMap { subType => getInnerTypes(subType, box) }
          case _                                        => Seq()
        }

        val intTpe = definitions.IntClass.tpe

        getInnerTypes(desc, false).toList map {
          case (boxed, OpaqueDescriptor(id, tpe, ref, _)) => {

            val udtSerTpe = appliedType(udtSerializerClass.tpe, List(tpe))
            mkVal(udtSerClassSym, "ser" + id, PRIVATE, false, udtSerTpe) { _ =>

              if (boxed) {
                val udtTpe = appliedType(udtClass.tpe, List(tpe))
                val udt = mkVal(udtSerClassSym, "udt", 0, false, udtTpe) { _ => ref }

                val range = Apply(Select(Select(Select(Select(Ident("scala"), "collection"), "immutable"), "Range"), "apply"), List(Literal(0), Select(Ident(udt.symbol), "numFields")))
                val idxMap = Apply(TypeApply(Select(range, "toArray"), List(TypeTree(intTpe))), List(Select(Select(Ident("reflect"), "Manifest"), "Int")))
                val ser = Apply(Select(Ident(udt.symbol), "createSerializer"), List(idxMap))

                Block(udt, ser)
              } else {
                Apply(Select(ref, "createSerializer"), List(Ident("idx" + id)))
              }
            }
          }
        }
      }

      private def mkPactHolders(udtSerClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

        def getFieldTypes(desc: UDTDescriptor): Seq[(Int, Type)] = desc match {
          case OpaqueDescriptor(_, _, _, _)                              => Seq()
          case PrimitiveDescriptor(id, _, _, sym)                        => Seq((id, sym.tpe))
          case ListDescriptor(id, _, _, _, _, elem: PrimitiveDescriptor) => Seq((id, appliedType(pactListClass.tpe, List(elem.wrapperClass.tpe))))
          case ListDescriptor(id, _, _, _, _, elem)                      => Seq((id, appliedType(pactListClass.tpe, List(pactRecordClass.tpe))))
          case CaseClassDescriptor(_, _, _, _, getters)                  => getters flatMap { getter => getFieldTypes(getter.descr) }
          case BaseClassDescriptor(id, _, subTypes)                      => (id, pactIntegerClass.tpe) +: (subTypes flatMap { subType => getFieldTypes(subType) })
        }

        val inits = getFieldTypes(desc)
        val fields = inits map { case (id, tpe) => mkVar(udtSerClassSym, "w" + id, PRIVATE, true, tpe) { _ => New(TypeTree(tpe), List(List())) } } toList

        val readObject = mkMethod(udtSerClassSym, "readObject", PRIVATE, List(("in", definitions.getClass("java.io.ObjectInputStream").tpe)), definitions.UnitClass.tpe) { _ =>
          val first = Apply(Select(Ident("in"), "defaultReadObject"), Nil)
          val rest = inits map { case (id, tpe) => Assign(Ident("w" + id), New(TypeTree(tpe), List(List()))) }
          val ret = Literal(())
          Block(((first +: rest) :+ ret): _*)
        }

        fields :+ readObject
      }

      private def mkSerialize(udtSerClassSym: Symbol, desc: UDTDescriptor): Tree = {

        def mkChkIdx(id: Int): Tree = Apply(Select(Select(This(udtSerClassSym), "idx" + id), "$greater$eq"), List(Literal(0)))
        def mkChkNotNull(source: Tree, tpe: Type): Tree = if (tpe.isNotNull) EmptyTree else Apply(Select(source, "$bang$eq"), List(Literal(Constant(null))))
        def mkSetField(id: Int, source: Tree, target: Tree): Tree = Apply(Select(target, "setField"), List(Select(This(udtSerClassSym), "idx" + id), source))
        def mkSerOpaque(id: Int, source: Tree, target: Tree): Tree = Apply(Select(Select(This(udtSerClassSym), "ser" + id), "serialize"), List(source, target))

        mkMethod(udtSerClassSym, "serialize", OVERRIDE | FINAL, List(("item", desc.tpe), ("record", pactRecordClass.tpe)), definitions.UnitClass.tpe) { methodSym =>

          def gen(desc: UDTDescriptor, source: Tree, target: Tree): Seq[Tree] = desc match {

            case OpaqueDescriptor(id, tpe, _, false) => mkIf(mkChkNotNull(source, tpe), mkSerOpaque(id, source, target))

            case OpaqueDescriptor(id, tpe, _, true) => {
              // Warning: recursive types introduce re-entrant calls to serialize()

              // Persist the outer record prior to recursing, since the call
              // is going to reuse all the PactPrimitive wrappers that were 
              // needed *before* the recursion.
              val updTgt = Apply(Select(target, "updateBinaryRepresenation"), List())

              val rec = mkVal(methodSym, "inner", 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
              val ser = mkSerOpaque(id, source, Ident(rec.symbol))

              // Persist the new inner record after recursing, since the
              // current call is going to reuse all the PactPrimitive
              // wrappers that are needed *after* the recursion.
              val updRec = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())
              val set = mkSetField(id, Ident(rec.symbol), target)

              mkIf(mkChkNotNull(source, tpe), updTgt, rec, ser, updRec, set)
            }

            case PrimitiveDescriptor(id, _, _, _) => {
              val chkIdx = mkChkIdx(id)
              val ser = Apply(Select(Select(This(udtSerClassSym), "w" + id), "setValue"), List(source))
              val set = mkSetField(id, Select(This(udtSerClassSym), "w" + id), target)
              val ret = Literal(())

              mkIf(chkIdx, ser, set, ret)
            }

            case ListDescriptor(id, tpe, _, _, iter, elem) => {
              val chk = mkAnd(mkChkIdx(id), mkChkNotNull(source, tpe))
              val updTgt = elem match {
                case OpaqueDescriptor(_, _, _, true) => Apply(Select(target, "updateBinaryRepresenation"), List())
                case _                               => EmptyTree
              }
              val clear = Apply(Select(Select(This(udtSerClassSym), "w" + id), "clear"), List())
              val it = mkVal(methodSym, "it", 0, false, appliedType(definitions.IteratorClass.tpe, List(elem.tpe))) { _ => iter(source) }
              val loop = mkWhile(Select(Ident(it.symbol), "hasNext")) {
                elem match {

                  case PrimitiveDescriptor(_, _, _, wrapper) => {
                    val value = New(TypeTree(wrapper.tpe), List(List(Select(Ident(it.symbol), "next"))))
                    Apply(Select(Select(This(udtSerClassSym), "w" + id), "add"), List(value))
                  }

                  case OpaqueDescriptor(elemId, tpe, _, _) => {
                    val item = mkVal(methodSym, "item", 0, false, tpe) { _ => Select(Ident(it.symbol), "next") }
                    val rec = mkVal(methodSym, "record", 0, false, pactRecordClass.tpe) { _ => New(TypeTree(pactRecordClass.tpe), List(List())) }
                    val ser = mkSerOpaque(elemId, Ident(item.symbol), Ident(rec.symbol))
                    val upd = Apply(Select(Ident(rec.symbol), "updateBinaryRepresenation"), List())
                    val add = Apply(Select(Select(This(udtSerClassSym), "w" + id), "add"), List(Ident(rec.symbol)))
                    val addNull = Apply(Select(Select(This(udtSerClassSym), "w" + id), "add"), List(Literal(Constant(null))))
                    val stats = mkIf(mkChkNotNull(Ident(item.symbol), tpe), Seq(rec, ser, upd, add), Seq(addNull))

                    mkBlock((item +: stats): _*)
                  }
                }
              }
              val set = mkSetField(id, Select(This(udtSerClassSym), "w" + id), target)
              val ret = Literal(())

              mkIf(chk, updTgt, clear, it, loop, set, ret)
            }

            case CaseClassDescriptor(_, tpe, _, _, getters) => mkIf(mkChkNotNull(source, tpe), (getters flatMap { case FieldAccessor(sym, _, desc) => gen(desc, Select(source, sym), target) }): _*)

            case BaseClassDescriptor(id, tpe, subTypes) => {
              val chk = mkAnd(mkChkIdx(id), mkChkNotNull(source, tpe))
              val cases = subTypes.zipWithIndex.toList map {
                case (ccd, i) => {
                  val pat = Bind("inst", Typed(Ident("_"), TypeTree(ccd.tpe)))
                  val ser = Apply(Select(Select(This(udtSerClassSym), "w" + id), "setValue"), List(Literal(i)))
                  val set = mkSetField(id, Select(This(udtSerClassSym), "w" + id), target)
                  val code = gen(ccd, Ident("inst"), target) match {
                    case Seq(If(_, code, _)) => deBlock(code)
                    case code                => code
                  }
                  val body = (ser +: set +: code) :+ Literal(())

                  CaseDef(pat, EmptyTree, mkBlock(body: _*))
                }
              }
              mkIf(chk, Match(source, cases))
            }
          }

          Block(gen(desc, Ident("item"), Ident("record")) toList, Literal(()))
        }
      }

      private def mkDeserialize(udtSerClassSym: Symbol, desc: UDTDescriptor): Tree = {

        mkMethod(udtSerClassSym, "deserialize", OVERRIDE | FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { _ =>
          desc match {
            case PrimitiveDescriptor(_, _, default, _) => default
            case _                                     => Literal(Constant(null))
          }
        }
      }

      private def mkIdent(target: Symbol): Tree = Ident(target) setType target.tpe

      private def mkVar(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
        val valSym = owner.newValue(name) setFlag (flags | SYNTHETIC | MUTABLE) setInfo valTpe
        if (transient) valSym.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
        ValDef(valSym, value(valSym))
      }

      private def mkVal(owner: Symbol, name: String, flags: Long, transient: Boolean, valTpe: Type)(value: Symbol => Tree): Tree = {
        val valSym = owner.newValue(name) setFlag (flags | SYNTHETIC) setInfo valTpe
        if (transient) valSym.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
        ValDef(valSym, value(valSym))
      }

      private def mkValAndGetter(owner: Symbol, name: String, flags: Long, valTpe: Type)(value: Symbol => Tree): List[Tree] = {

        val valDef = mkVal(owner, name + " ", PRIVATE, false, valTpe) { value }

        val defDef = mkMethod(owner, name, flags | ACCESSOR, Nil, valTpe) { _ =>
          Select(This(owner), name + " ")
        }

        List(valDef, defDef)
      }

      private def mkVarAndLazyGetter(owner: Symbol, name: String, flags: Long, valTpe: Type)(value: Symbol => Tree): List[Tree] = {

        val varDef = mkVar(owner, name + " ", PRIVATE, false, valTpe) { _ => Literal(Constant(null)) }

        val defDef = mkMethod(owner, name, flags | ACCESSOR, Nil, valTpe) { defSym =>

          val chk = Apply(Select(Select(This(owner), name + " "), "$eq$eq"), List(Literal(Constant(null))))
          val init = Assign(Select(This(owner), name + " "), value(defSym))
          val ret = Select(This(owner), name + " ")
          Block(If(chk, init, EmptyTree), ret)
        }

        List(varDef, defDef)
      }

      private def mkWhile(cond: Tree)(body: Tree): Tree = {
        val lblName = unit.freshTermName("while")
        val jump = Apply(Ident(lblName), Nil)
        val block = body match {
          case Block(stats, expr) => new Block(stats :+ expr, jump)
          case _                  => new Block(List(body), jump)
        }
        LabelDef(lblName, Nil, If(cond, block, EmptyTree))
      }

      private def mkIf(cond: Tree, bodyT: Tree*): Seq[Tree] = mkIf(cond, bodyT, Seq(EmptyTree))

      private def mkIf(cond: Tree, bodyT: Seq[Tree], bodyF: Seq[Tree]): Seq[Tree] = cond match {
        case EmptyTree => filterNonEmpty(bodyT: _*)
        case _         => Seq(If(cond, mkBlock(bodyT: _*), mkBlock(bodyF: _*)))
      }

      private def mkBlock(items: Tree*): Tree = items match {
        case Seq(item) => item
        case _         => Block(filterNonEmpty(items: _*): _*)
      }

      private def deBlock(item: Tree): Seq[Tree] = item match {
        case Block(Nil, ret)         => Seq(ret)
        case Block(item :: Nil, ret) => Seq(item, ret)
        case Block(items, ret)       => (items :+ ret).toSeq
        case _                       => Seq(item)
      }

      private def filterNonEmpty(items: Tree*): Seq[Tree] = items filter {
        case EmptyTree => false
        case _         => true
      }

      private def mkAnd(cond1: Tree, cond2: Tree): Tree = cond1 match {
        case EmptyTree => cond2
        case _ => cond2 match {
          case EmptyTree => cond1
          case _         => gen.mkAnd(cond1, cond2)
        }
      }

      private def mkMethod(owner: Symbol, name: String, flags: Long, args: List[(String, Type)], ret: Type)(impl: Symbol => Tree): Tree = {

        val methodSym = owner.newMethod(name) setFlag (flags | SYNTHETIC)

        if (args.isEmpty)
          methodSym setInfo NullaryMethodType(ret)
        else
          methodSym setInfo MethodType(methodSym newSyntheticValueParams args.unzip._2, ret)

        val valParams = args map { case (name, tpe) => ValDef(methodSym.newValueParameter(NoPosition, name) setInfo tpe) }

        DefDef(methodSym, Modifiers(flags | SYNTHETIC), List(valParams), impl(methodSym))
      }

      private def mkClass(owner: Symbol, name: String, flags: Long, parents: List[Type])(members: Symbol => List[Tree]): Tree = {

        val classSym = {
          if (name == null)
            owner newAnonymousClass owner.pos
          else
            owner newClass (owner.pos, unit.freshTypeName(name))
        }

        classSym setFlag (flags | SYNTHETIC)
        classSym setInfo ClassInfoType(parents, newScope, classSym)

        val classMembers = members(classSym)
        classMembers foreach { m => classSym.info.decls enter m.symbol }

        ClassDef(classSym, Modifiers(flags | SYNTHETIC), List(Nil), List(Nil), classMembers, owner.pos)
      }

      private def applyTransformation(tree: Tree)(trans: Tree => Tree): Tree = {
        val transformer = new Transformer {
          override def transform(tree: Tree): Tree = super.transform(trans(tree))
        }

        transformer.transform(tree)
      }

      private def getRelevantStackLine(e: Throwable): String = {
        val lines = e.getStackTrace.map(_.toString)
        val relevant = lines filter { _.contains("eu.stratosphere") }
        relevant.headOption getOrElse e.getStackTrace.toString
      }
    }
  }
}

