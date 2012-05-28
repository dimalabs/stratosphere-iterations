package eu.stratosphere.pact4s.compiler

import scala.collection.mutable

import scala.tools.nsc.Global
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.symtab.Flags._
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

abstract class UDTGenerator(udtDescriptors: UDTDescriptors) extends PluginComponent with Transform with TypingTransformers {

  override val global: udtDescriptors.global.type = udtDescriptors.global

  import global._
  import udtDescriptors._

  override val phaseName = "Pact4s.UDTGenerator"

  override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) {

    private val genSites = getGenSites(unit)

    private var first = true

    override def transform(tree: Tree): Tree = {

      val isFirst = first
      first = false

      val showResult: Tree => Unit = { tree: Tree =>
        if (unit.toString.contains("WordCount.scala") && isFirst) {
          treeBrowsers.create().browse(tree)
        }
      }

      withObserver(showResult) {

        tree match {

          // HACK: Blocks are naked (no symbol), so there's no scope in which to insert new implicits. 
          //       Wrap the block in an anonymous class, process the tree, then unpack the result.
          case block: Block if genSites(tree).nonEmpty => {

            val (wrapper, unwrap) = mkBlockWrapper(block)
            unwrap(super.transform(wrapper))
          }

          // Insert generated classes
          case Template(parents, self, body) if genSites(tree).nonEmpty => {

            super.transform {
              val udtInstances = genSites(tree).toList map { desc => (desc, mkUdtInst(desc)) }
              localTyper.typed { treeCopy.Template(tree, parents, self, udtInstances.unzip._2 ::: body) }
            }
          }

          // Rerun implicit inference at call sites
          case TypeApply(s: Select, List(t)) if s.symbol == unanalyzedUdt => {

            super.transform {

              val udtTpe = appliedType(udtClass.tpe, List(t.tpe))
              safely(tree) { e => reporter.error(currentOwner.pos, "Error generating UDT[" + t.tpe + "]: " + e.getMessage() + " @ " + getRelevantStackLine(e)) } {

                val udtInst = analyzer.inferImplicit(tree, udtTpe, true, false, localTyper.context)

                udtInst.tree match {
                  case t if t.isEmpty || t.symbol == unanalyzedUdt => {
                    reporter.info(tree.pos, "Failed to apply " + udtTpe, true)
                    tree
                  }
                  case udtInst => {
                    reporter.info(tree.pos, "Applied " + udtInst.symbol.fullName + ": " + udtInst.tpe, true)
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

    private def mkBlockWrapper(site: Block): (Tree, Tree => Tree) = {

      safely[(Tree, Tree => Tree)](site: Tree, identity[Tree] _) { e => reporter.error(currentOwner.pos, "Error generating BlockWrapper in " + currentOwner + ": " + e.getMessage() + " @ " + getRelevantStackLine(e)) } {

        val wrapperSym = currentOwner newAnonymousClass currentOwner.pos
        wrapperSym setInfo ClassInfoType(List(definitions.ObjectClass.tpe), newScope, wrapperSym)

        val mods = FINAL | STABLE | SYNTHETIC
        val result = newTermName("result")
        val defSym = wrapperSym.newMethod(result) setFlag mods setInfo NullaryMethodType(site.expr.tpe)
        wrapperSym.info.decls enter defSym

        val defDef = DefDef(defSym, Modifiers(mods), Nil, site.changeOwner((currentOwner, wrapperSym)))
        val classDef = ClassDef(wrapperSym, Modifiers(FINAL | SYNTHETIC), List(Nil), List(Nil), List(defDef), wrapperSym.pos)

        genSites(classDef.impl) = genSites(site)
        genSites(site) = Set()

        val wrappedBlock = localTyper.typed {
          Select(Block(classDef, New(TypeTree(wrapperSym.tpe), List(List()))), "result")
        }

        (wrappedBlock, unwrapBlock(wrapperSym, result))
      }
    }

    private def unwrapBlock(wrapper: Symbol, result: Name)(tree: Tree): Tree = {

      treeBrowsers.create().browse(tree)

      safely(tree) { e => reporter.error(currentOwner.pos, "Error unwrapping BlockWrapper in " + currentOwner + ": " + e.getMessage() + " @ " + getRelevantStackLine(e)) } {

        val Select(Block(List(cd: ClassDef), _), _) = tree
        val ClassDef(_, _, _, Template(_, _, body)) = cd
        val Some(DefDef(_, _, _, _, _, rhs: Block)) = body find { item => item.hasSymbol && item.symbol.name == result }

        val newImplicits = body filter { m => m.hasSymbol && m.symbol.isImplicit }
        val newSyms = newImplicits map { _.symbol } toSet

        val trans = new Transformer {
          override def transform(tree: Tree): Tree = tree match {
            case sel: Select if sel.hasSymbol && newSyms.contains(sel.symbol) => Ident(sel.symbol) setType sel.tpe
            case tree => super.transform(tree)
          }
        }

        val rewiredRhs = trans.transform(rhs).changeOwner((wrapper, currentOwner))

        val rewiredImplicits = newImplicits map { imp =>
          val sym = imp.symbol
          val ValDef(_, _, _, rhs) = imp

          sym.resetFlag(PRIVATE | FINAL)
          sym.owner = currentOwner

          ValDef(sym, rhs) setType sym.tpe
        }

        // Sanity check - make sure we've properly cleaned up after ourselves
        val detectArtifactsTransformer = new Transformer {
          override def transform(tree: Tree): Tree = {
            var detected = false

            if (tree.hasSymbol && tree.symbol == wrapper)
              detected = true

            if (tree.tpe == null) {
              reporter.error(currentOwner.pos, "Unwrapped tree has no type: " + tree)
            } else {
              val tpeArtifacts = tree.tpe filter { tpe => tpe.typeSymbol == wrapper || tpe.termSymbol == wrapper }
              detected |= tpeArtifacts.nonEmpty
            }

            if (detected)
              reporter.error(currentOwner.pos, "Wrapper artifact detected: " + tree)

            super.transform(tree)
          }
        }

        detectArtifactsTransformer.transform {
          localTyper.typed {
            val Block(stats, expr) = rewiredRhs
            treeCopy.Block(rewiredRhs, rewiredImplicits ::: stats, expr)
          }
        }
      }
    }

    private def mkUdtInst(desc: UDTDescriptor): Tree = {

      safely(EmptyTree: Tree) { e => reporter.error(currentOwner.pos, "Error generating UDT[" + desc.tpe + "]: " + e.getMessage() + " @ " + getRelevantStackLine(e)) } {
        withObserver[Tree] { t => reporter.info(currentOwner.pos, "Generated " + t.symbol.fullName + "[" + desc.tpe + "] @ " + currentClass + " " + currentOwner + " : " + t, true) } {

          val udtTpe = appliedType(udtClass.tpe, List(desc.tpe))
          val valSym = currentClass.newValue(unit.freshTermName("pact4s udtInst")) setInfo udtTpe

          currentClass.info.decls enter valSym
          valSym setFlag (PRIVATE | FINAL | IMPLICIT | SYNTHETIC)

          val udtClassDef = mkUdtClass(valSym, desc)
          val rhs = Block(udtClassDef, New(TypeTree(udtClassDef.symbol.tpe), List(List())))

          localTyper.typed {
            ValDef(valSym, rhs)
          }
        }
      }
    }

    private def mkUdtClass(owner: Symbol, desc: UDTDescriptor): Tree = {

      val udtTpe = appliedType(udtClass.tpe, List(desc.tpe))
      val udtClassSym = owner newClass (owner.pos, unit.freshTypeName("UDTImpl")) setFlag (FINAL | SYNTHETIC)
      udtClassSym setInfo ClassInfoType(List(definitions.ObjectClass.tpe, udtTpe), newScope, udtClassSym)

      val members = mkFieldTypes(udtClassSym, desc) :+ mkCreateSerializer(udtClassSym, desc)

      localTyper.typed {
        ClassDef(udtClassSym, Modifiers(FINAL | SYNTHETIC), List(Nil), List(Nil), members, owner.pos)
      }
    }

    private def mkFieldTypes(udtClassSym: Symbol, desc: UDTDescriptor): List[Tree] = {

      val elemTpe = {
        val exVar = udtClassSym.newAbstractType(newTypeName("_$1")) setInfo TypeBounds.upper(pactValueClass.tpe)
        ExistentialType(List(exVar), appliedType(definitions.ClassClass.tpe, List(TypeRef(NoPrefix, exVar, Nil))))
      }

      val valTpe = definitions.arrayType(elemTpe)
      val valSym = udtClassSym.newValue(newTermName("fieldTypes ")) setFlag (PRIVATE | SYNTHETIC) setInfo valTpe

      def getFieldTypes(desc: UDTDescriptor): Seq[(Boolean, Tree)] = desc match {
        case OpaqueDescriptor(_, ref)                             => Seq((false, Select(ref, "fieldTypes"))) //Apply(Select(ref, "fieldTypes"), Nil)))
        case PrimitiveDescriptor(_, _, sym)                       => Seq((true, gen.mkClassOf(sym.tpe)))
        case ListDescriptor(_, _, PrimitiveDescriptor(_, _, sym)) => Seq((true, gen.mkClassOf(appliedType(pactListClass.tpe, List(sym.tpe)))))
        case ListDescriptor(_, _, _)                              => Seq((true, gen.mkClassOf(appliedType(pactListClass.tpe, List(pactRecordClass.tpe)))))
        case CaseClassDescriptor(_, _, _, getters)                => getters flatMap { getter => getFieldTypes(getter.descr) }
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

      val rhs = fieldSets match {
        case Seq(a) => a
        case as     => Apply(TypeApply(Select(Select(Ident("scala"), "Array"), "concat"), List(TypeTree(elemTpe))), as.toList)
      }

      val valDef = localTyper.typed {
        udtClassSym.info.decls enter valSym
        ValDef(valSym, rhs)
      }

      val defDef = localTyper.typed {
        val mods = OVERRIDE | FINAL | STABLE | ACCESSOR | SYNTHETIC
        val defSym = udtClassSym.newMethod(newTermName("fieldTypes")) setFlag mods setInfo NullaryMethodType(valTpe)
        udtClassSym.info.decls enter defSym

        DefDef(defSym, Modifiers(mods), Nil, Select(This(udtClassSym), "fieldTypes "))
      }

      List(valDef, defDef)
    }

    private def mkCreateSerializer(udtClassSym: Symbol, desc: UDTDescriptor): Tree = {
      val name = newTermName("createSerializer")
      val methodSym = udtClassSym.newMethod(name) setFlag (OVERRIDE | FINAL | SYNTHETIC)

      val indexMap = {
        val ss = methodSym.newValueParameter(NoPosition, "indexMap") setInfo appliedType(definitions.ArrayClass.tpe, List(definitions.IntClass.tpe))
        ValDef(ss) setType ss.tpe
      }

      val udtSer = mkUdtSerializerClass(methodSym, desc)
      val udtSerTpe = appliedType(udtSerializerClass.tpe, List(desc.tpe))
      val rhs = Block(udtSer, New(TypeTree(udtSer.symbol.tpe), List(List())))

      methodSym setInfo MethodType(methodSym newSyntheticValueParams List(indexMap.tpe), udtSerTpe)

      localTyper.typed {
        DefDef(methodSym, Modifiers(OVERRIDE | FINAL | SYNTHETIC), List(List(indexMap)), rhs)
      }
    }

    private def mkUdtSerializerClass(owner: Symbol, desc: UDTDescriptor): Tree = {
      val udtSerTpe = appliedType(udtSerializerClass.tpe, List(desc.tpe))
      val udtSerClassSym = owner newClass (owner.pos, unit.freshTypeName("UDTSerializerImpl")) setFlag (FINAL | SYNTHETIC)
      udtSerClassSym setInfo ClassInfoType(List(udtSerTpe), newScope, udtSerClassSym)

      val members = List(mkSerialize(udtSerClassSym, desc), mkDeserialize(udtSerClassSym, desc))
      members foreach { m => udtSerClassSym.info.decls enter m.symbol }

      localTyper.typed {
        ClassDef(udtSerClassSym, Modifiers(FINAL | SYNTHETIC), List(Nil), List(Nil), members, owner.pos)
      }
    }

    private def mkSerialize(udtSerClassSym: Symbol, desc: UDTDescriptor): Tree = {
      val name = newTermName("serialize")
      val methodSym = udtSerClassSym.newMethod(name) setFlag (OVERRIDE | FINAL | SYNTHETIC)

      val item = {
        val ss = methodSym.newValueParameter(NoPosition, "item") setInfo desc.tpe
        ValDef(ss) setType ss.tpe
      }

      val record = {
        val ss = methodSym.newValueParameter(NoPosition, "record") setInfo pactRecordClass.tpe
        ValDef(ss) setType ss.tpe
      }

      methodSym setInfo MethodType(methodSym newSyntheticValueParams List(item.tpe, record.tpe), definitions.UnitClass.tpe)

      localTyper.typed {
        DefDef(methodSym, Modifiers(OVERRIDE | FINAL | SYNTHETIC), List(List(item, record)), Literal(()))
      }
    }

    private def mkDeserialize(udtSerClassSym: Symbol, desc: UDTDescriptor): Tree = {
      val name = newTermName("deserialize")
      val methodSym = udtSerClassSym.newMethod(name) setFlag (OVERRIDE | FINAL | SYNTHETIC)

      val record = {
        val ss = methodSym.newValueParameter(NoPosition, "record") setInfo pactRecordClass.tpe
        ValDef(ss) setType ss.tpe
      }

      methodSym setInfo MethodType(methodSym newSyntheticValueParams List(record.tpe), desc.tpe)

      val rhs = desc match {
        case PrimitiveDescriptor(_, default, _) => default
        case _                                  => Literal(Constant(null))
      }

      localTyper.typed {
        DefDef(methodSym, Modifiers(OVERRIDE | FINAL | SYNTHETIC), List(List(record)), rhs)
      }
    }

    private def safely[T](default: => T)(onError: Throwable => Unit)(block: => T): T = {
      try {
        block
      } catch {
        case e => { onError(e); default }
      }
    }

    private def withObserver[T](obs: T => Unit)(block: => T): T = {
      val ret = block
      obs(ret)
      ret
    }

    private def getRelevantStackLine(e: Throwable): String = {
      val lines = e.getStackTrace.map(_.toString)
      val relevant = lines filter { _.contains("eu.stratosphere") }
      relevant.headOption getOrElse e.getStackTrace.toString
    }
  }
}

