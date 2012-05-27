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

        val udtInstances = genSites(tree).toList map { desc => (desc, mkUdtInst(desc)) }

        super.transform {
          tree match {

            // Insert generated classes
            case Block(stats, expr)            => treeCopy.Block(tree, udtInstances.unzip._2 ::: stats, expr)
            case Template(parents, self, body) => treeCopy.Template(tree, parents, self, udtInstances.unzip._2 ::: body)

            // Rerun implicit inference at call sites
            case TypeApply(s: Select, List(t)) if s.symbol == unanalyzedUdt => {

              val udtTpe = appliedType(udtClass.tpe, List(t.tpe))
              safely[Tree] { e => reporter.error(currentOwner.pos, "Error generating UDT[" + t.tpe + "]: " + e.getMessage() + " @ " + e.getStackTrace.find(_.getClassName.startsWith("eu.stratosphere")).getOrElse(e.getStackTrace.head)); tree } {

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

            case _ => tree
          }
        }
      }
    }

    private def mkUdtInst(desc: UDTDescriptor): Tree = {

      safely[Tree] { e => reporter.error(currentOwner.pos, "Error generating UDT[" + desc.tpe + "]: " + e.getMessage() + " @ " + e.getStackTrace.find(_.getClassName.startsWith("eu.stratosphere")).getOrElse(e.getStackTrace.headOption.getOrElse("No Stack Trace"))); EmptyTree } {
        withObserver[Tree] { t => reporter.info(currentOwner.pos, "Generated " + t.symbol.fullName + "[" + desc.tpe + "]: " + t, true) } {

          val udtTpe = appliedType(udtClass.tpe, List(desc.tpe))
          val valSym = currentOwner.newValue(unit.freshTermName("udtInst")) setFlag (FINAL | IMPLICIT | SYNTHETIC) setInfo udtTpe
          currentOwner.info.decls enter valSym

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

    private def safely[T](onError: Throwable => T)(block: => T): T = {
      try {
        block
      } catch {
        case e => onError(e)
      }
    }

    private def withObserver[T](obs: T => Unit)(block: => T): T = {
      val ret = block
      obs(ret)
      ret
    }

    private def idempotently(value: Boolean, setter: Boolean => Unit)(block: => Unit): Unit = {
      if (value) {
        setter(false)
        block
      }
    }

    private def preserving[T, U](value: T, setter: T => Unit)(block: => U): U = {
      val ret = block
      setter(value)
      ret
    }
  }
}

