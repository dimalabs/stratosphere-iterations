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

import eu.stratosphere.pact4s.compiler.udtgen.UDTClassGenerators

trait UDTGenSiteTransformers extends UDTClassGenerators { this: Pact4sPlugin =>

  import global._

  trait UDTGenSiteTransformer extends Pact4sTransform with UDTGenSiteParticipant {

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with LoggingTransformer with TreeGenerator with UDTClassGenerator {

      private val genSites = getSites(unit)
      private val unitRoot = new EagerAutoSwitch[Tree] { override def guard = unit.toString.contains("Test.scala") }

      override def transform(tree: Tree): Tree = {

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
            case TypeApply(s: Select, List(t)) if s.symbol == defs.unanalyzedUdt => {

              super.transform {

                safely(tree) { e => "Error applying UDT[" + t.tpe + "]: " + getMsgAndStackLine(e) } {

                  val udtInst = analyzer.inferImplicit(tree, defs.mkUdtOf(t.tpe), true, false, localTyper.context)

                  udtInst.tree match {
                    case t if t.isEmpty || t.symbol == defs.unanalyzedUdt => {
                      val availUdts = localTyper.context.implicitss.flatten.map(m => m.name.toString + ": " + m.tpe.toString).filter(_.startsWith("udtInst")).sorted.mkString(", ")
                      val implicitCount = localTyper.context.implicitss.flatten.length
                      Error.report("Failed to apply " + defs.mkUdtOf(t.tpe) + ". Total Implicits: " + implicitCount + ". Available UDTs: " + availUdts)
                      tree
                    }
                    case udtInst => {
                      Debug.report("Applied " + udtInst.symbol.fullName + ": " + udtInst.tpe)
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

            val privateFlag = if (owner.isClass) Flags.PRIVATE else 0

            val List(valDef, defDef) = mkVarAndLazyGetter(owner, freshTermName("udtInst(") + ")", privateFlag | Flags.IMPLICIT, defs.mkUdtOf(desc.tpe)) { defSym =>

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
    }
  }
}

