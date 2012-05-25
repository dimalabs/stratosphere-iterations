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
    private val udtClasses = collection.mutable.Map[Type, (Symbol, UDTDescriptor)]()

    override def transform(tree: Tree): Tree = super.transform(insertAtSite(tree, genSites(tree) map { mkUdtClass(tree, _) }))

    private def insertAtSite(site: Tree, udtClasses: Iterable[Tree]): Tree = site match {
      case _ if udtClasses.isEmpty       => site
      case Block(stats, expr)            => treeCopy.Block(site, udtClasses.toList ::: stats, expr)
      case Template(parents, self, body) => treeCopy.Template(site, parents, self, udtClasses.toList ::: body)
    }

    private def mkUdtClass(site: Tree, desc: UDTDescriptor): Tree = {

      safely[Tree] { e => reporter.error(site.pos, "Error generating UDT[" + desc.tpe + "]: " + e.getMessage() + " @ " + e.getStackTrace.find(_.getClassName.startsWith("eu.stratosphere")).getOrElse(e.getStackTrace.head)); EmptyTree } {
        withObserver[Tree] { t => reporter.info(site.pos, "Generated UDT[" + desc.tpe + "]: " + t, true) } {

          val udtTpe = appliedType(udtClass.tpe, List(desc.tpe))
          val udtClassSym = currentOwner newClass (site.pos, unit.freshTypeName("UDTImpl")) setFlag (FINAL | SYNTHETIC)
          udtClassSym setInfo ClassInfoType(List(definitions.ObjectClass.tpe, udtTpe), newScope, udtClassSym)

          val members = List(mkFieldTypes(udtClassSym, desc), mkCreateSerializer(udtClassSym, desc))
          members foreach { m => udtClassSym.info.decls enter m.symbol }

          localTyper.typed {
            ClassDef(udtClassSym, Modifiers(FINAL | SYNTHETIC), List(Nil), List(Nil), members, site.pos)
          }
        }
      }
    }

    private def mkFieldTypes(udtClassSym: Symbol, desc: UDTDescriptor): Tree = {
      val name = newTermName("fieldTypes")

      //val valTpe = udtClass.tpe.members find { _.name.toString == "fieldTypes" } map { _.tpe.resultType } get
      val valTpe = {
        val exVar = udtClassSym.newAbstractType(newTypeName("_$1")) setInfo TypeBounds.upper(pactValueClass.tpe)
        TypeRef(definitions.ArrayClass.tpe.prefix, definitions.ArrayClass, List(
          ExistentialType(List(exVar), TypeRef(definitions.ClassClass.tpe.prefix, definitions.ClassClass, List(
            TypeRef(NoPrefix, exVar, List()))
          ))
        ))
      }
      val valSym = udtClassSym.newValue(name) setFlag (OVERRIDE | FINAL | SYNTHETIC) setInfo valTpe

      localTyper.typed {
        ValDef(valSym, New(TypeTree(valTpe), List(List(Literal(0)))))
      }
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
        val ss = methodSym.newValueParameter(NoPosition, "record") setInfo desc.tpe
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
        val ss = methodSym.newValueParameter(NoPosition, "record") setInfo desc.tpe
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
      obs(block)
      ret
    }
  }
}