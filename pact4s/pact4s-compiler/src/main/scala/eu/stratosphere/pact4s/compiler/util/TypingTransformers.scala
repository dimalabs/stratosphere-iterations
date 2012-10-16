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

package eu.stratosphere.pact4s.compiler.util

import scala.tools.nsc.Global
import scala.tools.nsc.transform.{ TypingTransformers => NscTypingTransformers }

trait TypingTransformers { this: HasGlobal =>

  import global._

  private[TypingTransformers] val nscTypingTransformers = new NscTypingTransformers with InheritsGlobal

  val neverInfer = Set[Symbol]()

  trait TypingVisitor extends HasCompilationUnit with HasPosition { this: HasCompilationUnit =>

    protected def localTyper: analyzer.Typer
    protected var curTree: Tree

    private var envs: Seq[(Tree, Scope)] = Seq()

    protected def curPath: Seq[Tree] = envs.unzip._1
    protected def curPos: Position = curPath find { _.pos != NoPosition } map { _.pos } getOrElse NoPosition

    private def treeFromImplicitResult(result: analyzer.SearchResult, dontInfer: Set[Symbol]): Option[Tree] = result.tree match {
      case EmptyTree                         => None
      case t if dontInfer.contains(t.symbol) => None
      case t                                 => Some(t)
    }

    // It doesn't matter what we pass here for the tree parameter - we're not interested in determining types based on implicit resolution
    protected def inferImplicitInst(tpe: Type, dontInfer: Set[Symbol] = neverInfer): Option[Tree] = treeFromImplicitResult(analyzer.inferImplicit(EmptyTree, tpe, true, false, localTyper.context), dontInfer)
    protected def inferImplicitView(tpe: Type, dontInfer: Set[Symbol] = neverInfer): Option[Tree] = treeFromImplicitResult(analyzer.inferImplicit(EmptyTree, tpe, true, true, localTyper.context), dontInfer)

    protected def inferImplicitView(from: Type, to: Type): Option[Tree] = inferImplicitView(from, to, neverInfer)
    protected def inferImplicitView(from: Type, to: Type, dontInfer: Set[Symbol]): Option[Tree] = inferImplicitView(definitions.functionType(List(from), to), dontInfer)

    protected def inferImplicitInsts(tpes: List[Type], dontInfer: Set[Symbol] = neverInfer): Either[List[Type], List[Tree]] = {

      val (errs, insts) = tpes.foldRight((Nil: List[Type], Nil: List[Tree])) { (tpe, ret) =>
        inferImplicitInst(tpe) match {
          case None      => ret.copy(_1 = tpe :: ret._1)
          case Some(ref) => ret.copy(_2 = ref :: ret._2)
        }
      }

      errs match {
        case Nil => Right(insts)
        case _   => Left(errs)
      }
    }

    protected def pre(tree: Tree) = {
      curTree = tree
      envs = (tree, localTyper.context.scope) +: envs
    }

    protected def post(tree: Tree) = {
      val pos = curPos
      tree filter { _.pos eq NoPosition } foreach { _.pos = pos }

      setScope(envs.head._2)
      envs = envs.tail
    }

    private def setScope(scope: Scope) = {
      if (localTyper.context.scope ne scope) {
        localTyper.context.scope = scope
        localTyper.context.resetCache()
      }
    }

    private[TypingTransformers] def enterLocalImplicits(tree: Tree): Unit = {

      val stats = tree match {
        case Block(stats, _)               => stats
        case Function(params, _)           => params
        case DefDef(_, _, _, params, _, _) => params.flatten
        case _                             => Nil
      }

      val syms = stats filter { stat => stat.hasSymbol && stat.symbol.isImplicit } map { _.symbol }

      syms match {
        case Nil => localTyper.context.scope
        case _ => {
          val scope = localTyper.context.scope.cloneScope
          syms foreach scope.enter
          setScope(scope)
        }
      }
    }
  }

  abstract class TypingTransformer(protected val unit: CompilationUnit) extends nscTypingTransformers.TypingTransformer(unit) with TypingVisitor with HasCompilationUnit {

    final override def transform(tree: Tree): Tree = {
      pre(tree)
      val ret = apply(tree) setPos tree.pos
      post(ret)
      ret
    }

    protected def apply(tree: Tree): Tree = {
      enterLocalImplicits(tree)
      super.transform(tree)
    }
  }

  abstract class TypingTraverser(protected val unit: CompilationUnit) extends nscTypingTransformers.TypingTransformer(unit) with TypingVisitor with HasCompilationUnit {

    final override def transform(tree: Tree): Tree = {
      pre(tree)
      apply(tree)
      post(tree)
      tree
    }

    protected def apply(tree: Tree): Unit = {
      enterLocalImplicits(tree)
      super.transform(tree)
    }
  }
}

