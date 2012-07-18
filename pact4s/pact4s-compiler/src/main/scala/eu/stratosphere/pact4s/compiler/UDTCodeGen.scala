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

                val udtInstances = genSites(tree).toList map { desc => mkUdtInst(tree.symbol, desc) }
                log(Debug) { "GenSite " + tree.symbol + " defines:   " + tree.symbol.tpe.members.filter(_.isImplicit).map(_.name.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ") }

                localTyper.typed { treeCopy.ClassDef(tree, mods, name, tparams, treeCopy.Template(template, parents, self, udtInstances ::: body)) }
              }
            }

            // Rerun implicit inference at call sites bound to unanalyzedUdt
            case TypeApply(s: Select, List(t)) if s.symbol == unanalyzedUdt => {

              super.transform {

                safely(tree) { e => "Error applying UDT[" + t.tpe + "]: " + e.getMessage() + " @ " + getRelevantStackLine(e) } {

                  val udtTpe = appliedType(udtClass.tpe, List(t.tpe)) map { t => if (t.typeSymbol.isMemberOf(definitions.getModule("java.lang"))) t.normalize else t }
                  val udtInst = analyzer.inferImplicit(tree, udtTpe, true, false, localTyper.context)

                  udtInst.tree match {
                    case t if t.isEmpty || t.symbol == unanalyzedUdt => {
                      log(Error) { "Failed to apply " + udtTpe + ". Available UDTs: " + localTyper.context.implicitss.flatten.map(_.name.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ") }
                      tree
                    }
                    case udtInst => {
                      log(Debug) { "Applied " + udtInst.symbol.fullName + ": " + udtInst.tpe }
                      localTyper.typed {
                        Typed(udtInst, TypeTree(udtTpe))
                      }
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

        visually (detected) {
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

      private def mkUdtInst(owner: Symbol, desc: UDTDescriptor): Tree = {

        safely(EmptyTree: Tree) { e => "Error generating UDT[" + desc.tpe + "]: " + e.getMessage() + " @ " + getRelevantStackLine(e) } {
          verbosely { t: Tree => "Generated " + t.symbol.fullName + "[" + desc.tpe + "] @ " + owner + " : " + t } {

            val udtTpe = appliedType(udtClass.tpe, List(desc.tpe))

            val tree = mkVal(owner, unit.freshTermName("udtInst(") + ")", PRIVATE | IMPLICIT, udtTpe) { valSym =>

              val udtClassDef = mkUdtClass(valSym, desc)
              Block(udtClassDef, Typed(New(TypeTree(udtClassDef.symbol.tpe), List(List())), TypeTree(udtTpe)))
            }

            owner.info.decls enter tree.symbol

            // Why is the UnCurry phase unhappy if we don't run the typer here?
            // We're already running it for the enclosing ClassDef...
            localTyper.typed { tree }
          }
        }
      }

      private def mkUdtClass(owner: Symbol, desc: UDTDescriptor): Tree = {

        mkClass(owner, "UDTImpl", FINAL, List(definitions.ObjectClass.tpe, appliedType(udtClass.tpe, List(desc.tpe)))) { classSym =>
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

          // TODO (Joe): Tag summation types
          // TODO (Joe): Box recursive types
          def getFieldTypes(desc: UDTDescriptor): Seq[(Boolean, Tree)] = desc match {
            case OpaqueDescriptor(_, ref)                             => Seq((false, Select(ref, "fieldTypes")))
            case PrimitiveDescriptor(_, _, sym)                       => Seq((true, gen.mkClassOf(sym.tpe)))
            case ListDescriptor(_, _, PrimitiveDescriptor(_, _, sym)) => Seq((true, gen.mkClassOf(appliedType(pactListClass.tpe, List(sym.tpe)))))
            case ListDescriptor(_, _, _)                              => Seq((true, gen.mkClassOf(appliedType(pactListClass.tpe, List(pactRecordClass.tpe)))))
            case CaseClassDescriptor(_, _, _, getters)                => getters flatMap { getter => getFieldTypes(getter.descr) }
            case BaseClassDescriptor(_, subTypes)                     => Seq() // not implemented
          }

          val fieldSets = getFieldTypes(desc).foldRight(Seq[(Boolean, Seq[Tree])]()) { (f, z) =>
            val (compose, field) = f
            z match {
              case (true, group) :: rest if compose => (true, field +: group) +: rest
              case _                                => (compose, Seq(field)) +: z
            }
          } map {
            case (false, Seq(ref)) => ref
            case (true, group)     => ArrayValue(TypeTree(elemTpe), group.toList)
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
          Block(udtSer, Typed(New(TypeTree(udtSer.symbol.tpe), List(List())), TypeTree(udtSerTpe)))
        }
      }

      private def mkUdtSerializerClass(owner: Symbol, desc: UDTDescriptor): Tree = {

        mkClass(owner, "UDTSerializerImpl", FINAL, List(appliedType(udtSerializerClass.tpe, List(desc.tpe)))) { classSym =>
          List(mkSerialize(classSym, desc), mkDeserialize(classSym, desc))
        }
      }

      private def mkSerialize(udtSerClassSym: Symbol, desc: UDTDescriptor): Tree = {
        mkMethod(udtSerClassSym, "serialize", OVERRIDE | FINAL, List(("item", desc.tpe), ("record", pactRecordClass.tpe)), definitions.UnitClass.tpe) { _ =>
          Literal(())
        }
      }

      private def mkDeserialize(udtSerClassSym: Symbol, desc: UDTDescriptor): Tree = {

        mkMethod(udtSerClassSym, "deserialize", OVERRIDE | FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { _ =>
          desc match {
            case PrimitiveDescriptor(_, default, _) => default
            case _                                  => Literal(Constant(null))
          }
        }
      }

      private def mkIdent(target: Symbol): Tree = Ident(target) setType target.tpe

      private def mkVal(owner: Symbol, name: String, flags: Int, valTpe: Type)(value: Symbol => Tree): Tree = {
        val valSym = owner.newValue(name) setFlag (flags | SYNTHETIC) setInfo valTpe
        ValDef(valSym, value(valSym))
      }

      private def mkMethod(owner: Symbol, name: String, flags: Int, args: List[(String, Type)], ret: Type)(impl: Symbol => Tree): Tree = {

        val methodSym = owner.newMethod(name) setFlag (flags | SYNTHETIC)

        if (args.isEmpty)
          methodSym setInfo NullaryMethodType(ret)
        else
          methodSym setInfo MethodType(methodSym newSyntheticValueParams args.unzip._2, ret)

        val valParams = args map { case (name, tpe) => ValDef(methodSym.newValueParameter(NoPosition, name) setInfo tpe) }

        DefDef(methodSym, Modifiers(flags | SYNTHETIC), List(valParams), impl(methodSym))
      }

      private def mkValAndGetter(owner: Symbol, name: String, flags: Int, valTpe: Type)(value: Symbol => Tree): List[Tree] = {

        val valDef = mkVal(owner, name + " ", PRIVATE, valTpe) { value }

        val defDef = mkMethod(owner, name, flags | ACCESSOR, Nil, valTpe) { _ =>
          Select(This(owner), name + " ")
        }

        List(valDef, defDef)
      }

      private def mkClass(owner: Symbol, name: String, flags: Int, parents: List[Type])(members: Symbol => List[Tree]): Tree = {

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

