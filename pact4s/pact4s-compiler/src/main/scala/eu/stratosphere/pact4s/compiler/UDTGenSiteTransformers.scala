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

  trait UDTGenSiteTransformer extends Pact4sComponent with UDTGenSiteParticipant {

    override def newTransformer(unit: CompilationUnit) = new TypingTransformer(unit) with Logger with TreeGenerator with UDTClassGenerator {

      private val genSites = getSites(unit)

      override def apply(tree: Tree): Tree = {

        super.apply {
          tree match {

            // Generate UDT classes and inject them into the AST.

            // Blocks are naked (no symbol), so new implicits
            // must be manually inserted into the typer's context.            
            case Block(stats, ret) if genSites(tree).nonEmpty => {

              val unmangledStats = stats match {
                case List(Literal(Constant(()))) => Nil
                case _                           => stats
              }

              val udtInstances = genSites(tree).toList flatMap { mkUdtInst(currentOwner, _) }
              val newStats = udtInstances ++ unmangledStats

              verbosely[Tree] { tree => "GenSite Block[" + posString(tree.pos) + "] defines: " + availUdts(newStats map { _.symbol }) } {
                localTyper.typed { treeCopy.Block(tree, newStats, ret) }
              }
            }

            // If a DefDef or a Function is a gen site, then it's rhs is
            // not a Block (otherwise the block would be the gen site).
            // The rhs must therefore be transformed into a block so that
            // we have somewhere to insert new statements.
            case Function(vparams, rhs) if genSites(tree).nonEmpty => {
              val mangledRhs = Block(mkUnit, rhs) setPos rhs.pos
              genSites(mangledRhs) = genSites(tree)
              localTyper.typed { treeCopy.Function(tree, vparams, mangledRhs) }
            }

            case DefDef(mods, name, tparams, vparamss, tpt, rhs) if genSites(tree).nonEmpty => {
              val mangledRhs = Block(mkUnit, rhs) setPos rhs.pos
              genSites(mangledRhs) = genSites(tree)
              localTyper.typed { treeCopy.DefDef(tree, mods, name, tparams, vparamss, tpt, mangledRhs) }
            }

            // Classes have a body in which to insert new statements and a scope
            // in which to insert new implicits, so no mangling is needed here.
            case ClassDef(mods, name, tparams, template @ Template(parents, self, body)) if genSites(tree).nonEmpty => {

              verbosely[Tree] { tree => "GenSite " + tree.symbol + " defines: " + availUdts(tree.symbol.tpe.members) } {
                val udtInstances = genSites(tree).toList flatMap { mkUdtInst(tree.symbol, _) }
                localTyper.typed { treeCopy.ClassDef(tree, mods, name, tparams, treeCopy.Template(template, parents, self, udtInstances ::: body)) }
              }
            }

            // Rerun implicit inference at call sites bound to unanalyzedUdt
            case TypeApply(s: Select, List(t)) if s.symbol == defs.unanalyzedUdt => {

              safely(tree, false) { e => "Error applying UDT[" + t.tpe + "]: " + getMsgAndStackLine(e) } {

                inferImplicitInst(defs.mkUdtOf(t.tpe)) match {
                  case None => {
                    val implicitCount = localTyper.context.implicitss.flatten.length
                    Error.report("Failed to apply " + defs.mkUdtOf(t.tpe) + ". Total Implicits: " + implicitCount + ". Available UDTs: " + availUdts(Nil))
                    tree
                  }
                  case Some(udtInst) => {
                    Debug.report("Applied " + udtInst.symbol.fullName + ": " + udtInst.tpe)
                    localTyper.typed { udtInst }
                  }
                }
              }
            }

            case _ => tree
          }
        }
      }

      private def availUdts(locals: List[Symbol]): String = {
        val ctx = localTyper.context.implicitss.flatten.map(m => m.sym)
        val loc = locals filter { sym => (sym ne NoSymbol) && sym.isImplicit }
        (ctx union loc).map(sym => sym.name + ": " + sym.tpe).filter(_.startsWith("udtInst")).sorted.mkString(", ")
      }

      private def mkUdtInst(owner: Symbol, desc: UDTDescriptor): List[Tree] = {

        verbosely[List[Tree]] { case l => "Generated " + l.head.symbol.fullName + "[" + desc.tpe + "] @ " + owner } {

          val privateFlag = if (owner.isClass) Flags.PRIVATE else 0

          val stats = mkVarAndLazyGetter(owner, unit.freshTermName("udtInst(") + ")", privateFlag | Flags.IMPLICIT, defs.mkUdtOf(desc.tpe)) { defSym =>

            val udtClassDef = mkUdtClass(defSym, desc)
            val udtInst = New(TypeTree(udtClassDef.symbol.tpe), List(List()))
            val rhs = Block(udtClassDef, udtInst)

            safely(rhs, true) { e => "Error generating UDT[" + desc.tpe + "]: " + getMsgAndStackLine(e) } {
              localTyper.typed { rhs }
            }
          }

          if (owner.isClass) {
            stats foreach { stat => owner.info.decls enter stat.symbol }
          }

          stats
        }
      }
    }
  }
}

