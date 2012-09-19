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

package eu.stratosphere.pact4s.compiler.util.treeReducers

import eu.stratosphere.pact4s.compiler.util._

trait TreeReducerImpls { this: TreeReducers with HasGlobal with TreeGenerators =>

  private val treeGen = new TreeGenerator {}

  import global._
  import treeGen._

  class TreeReducer(val strictBlocks: Boolean) {

    import TreeReducer._
    import TreeReducer.Extractors._

    private object callStack {

      private val marked = new util.DynamicVariable(Set[Symbol]())

      /**
       * Evaluates the block if and only if sym is not
       * already on the call stack. This prevents recursion.
       *
       * @param sym   the symbol representing the function to be evaluated
       * @param fun   the function to be evaluated
       * @return      Some(fun) if sym is not already on the call stack
       */
      def enter(sym: Symbol)(fun: => Tree): Option[Tree] = {

        if (marked.value.contains(sym)) {
          None
        } else {
          marked.withValue(marked.value + sym) {
            Some(fun)
          }
        }
      }
    }

    /*
     * A non-deterministic expression was encountered. Reduce its branches in a fresh 
     * copy of the current environment in order to track accesses and mutations, then 
     * update the current environment as follows:
     * 
     * (1) If a member was accessed in any branch, then mark it as accessed
     * (2) If a member was modified in any branch, then mark it as mutated and unknown
     */
    private val diverging = new util.DynamicVariable(Set[Symbol]())

    private def diverge(env: Environment, branches: List[Tree], until: Symbol): List[Tree] = {

      val marker = until match {
        case NoSymbol => NoSymbol.newValue("anonanchor$")
        case until    => until
      }

      if (diverging.value.contains(marker)) {
        
        branches
        
      } else {
        
        diverging.withValue(diverging.value + marker) {
          
          val orig = env.copy

          branches map { expr =>
            val exprEnv = orig.copy
            val ret = reduce(expr, exprEnv)
            env.merge(exprEnv)
            ret
          }
        }
      }
    }

    /*
     * Create an anonymous function with an environment and a symbol. If the
     * function has multiple parameter lists, then a curried function is
     * returned. The outermost function will carry the provided symbol, and 
     * inner functions will carry idempotently generated anonymous symbols.
     * The symbols allow for recursion detection even when the recursion is 
     * not anchored by a named definition (for example, the Y combinator).
     */
    private def mkClosure(evalEnv: => Environment, sym: Symbol, paramss: List[List[ValDef]], rhs: Tree): Closure = {

      val fun = paramss match {
        case Nil           => new Closure { override def unpacked = (evalEnv, Nil, rhs) }
        case params :: Nil => new Closure { override def unpacked = (evalEnv, params, rhs) }
        case params :: rest => new Closure {
          override def unpacked = {
            val env = evalEnv
            val inner = mkClosure(env, SymbolFactory.makeAnonFun(sym), rest, rhs)
            (env, params, inner)
          }
        }
      }

      fun setSymbol sym setPos sym.pos
    }

    def reduce(tree: Tree, env: Environment): Tree = tree match {

      case PackageDef(pid, stats) => {
        val pkg = env.makeChild setSymbol tree.symbol
        env(tree.symbol) = pkg
        stats foreach { reduce(_, pkg) }
        mkUnit
      }

      case ObjectDef(mods, name, tpts, impl @ Template(ps, self, body)) => {

        val classEnv = {
          // Evaluate this tree as a normal class definition
          try {
            tree.symbol.resetFlag(Flags.MODULE)
            reduce(tree, env.makeChild)
          } finally {
            tree.symbol.setFlag(Flags.MODULE)
          }
        }

        // Get the primary constructor body
        val ctorSym = tree.symbol.primaryConstructor
        val body = classEnv match {
          case classEnv: Environment => classEnv(ctorSym) match {
            case Closure(_, Nil, body) => body
            case _                     => throw new UnsupportedOperationException("Invalid object definition")
          }
          case _ => throw new UnsupportedOperationException("Invalid object definition")
        }

        /* 
         * Conceptually, an Object is a lazily evaluated singleton instance of
         * a class with an otherwise inaccessible constructor. Normal lazy eval
         * won't work here though, because the instance's constructor can reference
         * the instance via selection on an outer class. Lazy eval won't overwrite
         * the function with the result until the result has been fully evaluated,
         * so an outer self reference in the constructor would be treated as an 
         * infinite recursion rather than a convoluted way of saying 'this'. To
         * avoid that, the lazy def needs to overwrite itself with a pointer to 
         * the (uninitialized) instance before stepping into the ctor function.
         */

        val classSym = classEnv.symbol
        val inst = env.makeChild setSymbol SymbolFactory.makeInstanceOf(classSym)
        TypeOf(inst) = classSym

        val upd = Assign(Ident(classSym), inst)
        val ctor = mkClosure(inst, ctorSym, Nil, body)
        val rhs = Block(List(upd), ctor)

        env(classSym) = mkClosure(env, classSym.newValue("<objInit>"), Nil, rhs)
        mkUnit
      }

      case ClassDef(mods, name, tpts, impl @ Template(parents, self, body)) => {

        val classSym = tree.symbol
        val classEnv = env.makeChild setSymbol classSym
        env(classSym) = classEnv

        val (ctors, cstats) = body partition {
          case tree if tree.hasSymbolWhich(_.isConstructor) => true
          case _                                            => false
        }

        ctors foreach {

          case ctorDef @ DefDef(_, _, _, paramss, _, Block(ccstats, Literal(Constant(())))) => {

            val stats = ctorDef.symbol.isPrimaryConstructor match {

              /*
               * Defs and other statements in the class body become part of the primary ctor function's body. 
               * Evaluating this function instantiates the defs as closures around the instance and stores 
               * them as instance members. All statements will be evaluated in the instance environment.
               */
              case true => {
                val mixinCtors = classSym.mixinClasses map { sym => Apply(Select(Super(classSym, sym.name.toTypeName), "<init>") setSymbol sym.primaryConstructor, Nil) }
                ccstats ++ mixinCtors ++ cstats
              }

              case false => ccstats
            }

            // Tagging this as a ctor block (blocks don't usually have symbols)
            val body = new Block(stats, mkUnit) { override val hasSymbol = true }

            classEnv(ctorDef.symbol) = mkClosure(classEnv, ctorDef.symbol, paramss, body)
          }

          case _ => throw new UnsupportedOperationException("Invalid constructor definition")
        }

        mkUnit
      }

      case LazyAccessorDef(mods, name, tparams, paramss, tpt, rhs) => {

        val Block(List(Assign(_, init)), _) = rhs

        // Make a function which evaluates the expression and then overwrites itself with the result.
        val sel = Select(This(tree.symbol.owner), tree.symbol)
        val upd = Assign(sel, init)

        env(tree.symbol) = mkClosure(env, tree.symbol, Nil, Block(List(upd), sel))
        mkUnit
      }

      case ValDef(_, _, _, rhs)             => env(tree.symbol) = reduce(rhs, env); mkUnit
      case DefDef(_, _, _, paramss, _, rhs) => env(tree.symbol) = mkClosure({ env.makeChild }, tree.symbol, paramss, rhs); mkUnit

      // Ctor call site
      case Select(New(from), _) => reduce(Select(from, tree.symbol), env) match {

        case ctor @ Closure(classEnv, params, body) => {
          val inst = classEnv.makeChild setSymbol SymbolFactory.makeInstanceOf(classEnv.symbol)
          TypeOf(inst) = classEnv.symbol
          mkClosure(inst, ctor.symbol, List(params), body)
        }

        // TODO: make a function that initializes instance members as NonReducible 
        case ctor: NonReducible => NonReducible(ReductionError.CausedBy(ctor), tree)
        case _                  => throw new UnsupportedOperationException("Invalid constructor")
      }

      // Ctor call site within an auxilliary ctor
      case Select(This(_), _) if tree.symbol.isConstructor => env(tree.symbol) match {

        // When stepping from one ctor to another, keep the same instance environment
        case ctor @ Closure(_, params, body) => mkClosure(env, ctor.symbol, List(params), body)

        case _                               => throw new UnsupportedOperationException("Invalid constructor")
      }

      // Super ctor call site within a subclass
      case Select(Super(_, _), _) if tree.symbol.isConstructor => env.find(tree.symbol) match {

        case Some(ctor @ Closure(superClassEnv, params, body)) => {

          val flag = SymbolFactory.makeCtorFlag(superClassEnv.symbol)

          env.defines(flag) match {

            // This super class's ctor has already been called, so this call is a no-op
            case true => mkClosure(env.makeChild, ctor.symbol, ctor.getParams, mkUnit)

            case false => {

              // Mark that this super class's ctor has been called
              env(flag) = EmptyTree

              // Make a proxy to the instance environment with the super class as 
              // its parent, so that the super class's outer classes are accessible.
              val inst = env.asChildOf(superClassEnv) setSymbol SymbolFactory.makeInstanceOf(superClassEnv.symbol)
              mkClosure(inst, ctor.symbol, List(params), body)
            }
          }
        }

        // TODO: make a function that initializes instance members as NonReducible 
        case None => NonReducible(ReductionError.NoSource, tree)
        case _    => throw new UnsupportedOperationException("Invalid constructor")
      }

      // The program has already been type checked, so just unwrap type applications
      case Typed(expr, tpt)                                 => reduce(expr, env)
      case TypeApply(fun, args)                             => reduce(fun, env)

      // Calls to parameterless methods (and call-by-name arg refs) don't get Apply nodes, so reduce them on access
      case Closure(env, Nil, body)                          => reduce(body, env)

      // Types, literals, instances, unapplied closures, and non-reducible statements are already in WHNF
      case _: TypeDef | _: TypTree | _: Literal | EmptyTree => tree
      case _: Environment | _: Closure | _: NonReducible    => tree

      case Function(vparams, body)                          => mkClosure({ env.makeChild }, tree.symbol, List(vparams), body)

      case Ident(_) => env.findParent(_.defines(tree.symbol)) match {
        case Some(owner) => reduce(owner(tree.symbol), env)
        case _           => throw new UnsupportedOperationException("Invalid assignment")
      }

      case This(_) => env.findParent(_.symbol eq SymbolFactory.makeInstanceOf(tree.symbol)) match {
        case Some(inst) => inst
        case None       => throw new UnsupportedOperationException("Invalid this reference")
      }

      // Super call site within a subclass - ignore overriding
      case Select(Super(_, _), _) => reduce(env(tree.symbol), env)

      // Normal selection - respect overriding
      case Select(from, _) => reduce(from, env) match {

        case inst: Environment => {

          val sym = tree.symbol.overridingSymbol(TypeOf(inst)) match {
            case null | NoSymbol => tree.symbol
            case sym             => sym
          }

          reduce(inst(sym), env)
        }

        case inst: NonReducible => NonReducible(ReductionError.CausedBy(inst), Select(inst.expr, tree.symbol))
        case _                  => throw new UnsupportedOperationException("Invalid selection")
      }

      case Apply(fun, args) => reduce(fun, env) match {

        case rFun @ Closure(evalEnv, vparams, body) => {

          val rArgs = vparams.map(_.symbol).zip(args) map {
            case (sym, arg) if sym.isByNameParam => (sym, mkClosure(env, sym, Nil, arg)) // call-by-name: pass arg unevaluated
            case (sym, arg)                      => (sym, reduce(arg, env))
          }

          callStack.enter(rFun.symbol) {

            for ((sym, arg) <- rArgs)
              evalEnv(sym) = arg

            reduce(body, evalEnv)

          } getOrElse {

            diverge(evalEnv, List(body), rFun.symbol)
            NonReducible(ReductionError.Recursive, Apply(rFun, rArgs.map(_._2)))
          }
        }

        case rFun: NonReducible => NonReducible(ReductionError.CausedBy(rFun), Apply(rFun.expr, args map { reduce(_, env) }).copyAttrs(tree))
        case _                  => throw new UnsupportedOperationException("Invalid function application")
      }

      case Assign(lhs, rhs) => {

        val target = lhs match {

          case Ident(_) => env.findParent(_.defines(lhs.symbol)) match {
            case Some(target) => target
            case _            => throw new UnsupportedOperationException("Invalid assignment")
          }

          case Select(from, _) => reduce(from, env)
        }

        target match {
          // target(lhs.symbol) is always a var, so no need to worry about overriding
          case target: Environment  => target(lhs.symbol) = reduce(rhs, env); mkUnit

          case target: NonReducible => NonReducible(ReductionError.CausedBy(target), Assign(target.expr, reduce(rhs, env)))
          case _                    => throw new UnsupportedOperationException("Invalid assignment")
        }
      }

      case Block(stats, expr) => {

        val blockEnv = tree.hasSymbol match {
          case true  => env // body of a ctor function
          case false => env.makeChild // anonymous block or body of non-ctor function
        }

        val inits = stats flatMap {
          case stat: ValDef => Some(stat.copy(rhs = mkDefault(stat.symbol.tpe.typeSymbol)))
          case _            => None
        }

        val orderedStats = stats.zipWithIndex sortBy {
          case (_: ValDef, index)    => (1, index)
          case (_: MemberDef, index) => (0, index)
          case (_, index)            => (1, index)
        } map { _._1 }

        val rStats = (inits ++ orderedStats) map { reduce(_, blockEnv) }
        val rExpr = reduce(expr, blockEnv)

        val notReduced = rStats find {
          case _: NonReducible => true
          case _               => false
        }

        (strictBlocks, notReduced) match {
          case (true, Some(stat: NonReducible)) => NonReducible(ReductionError.CausedBy(stat), rExpr)
          case _                                => rExpr
        }
      }

      case If(cond, thenp, elsep) => reduce(cond, env) match {

        case Literal(Constant(true))  => reduce(thenp, env)
        case Literal(Constant(false)) => reduce(elsep, env)

        case cond => {

          val reason = cond match {
            case cond: NonReducible => ReductionError.CausedBy(cond)
            case _                  => ReductionError.NonDeterministic
          }

          val List(exprT, exprE) = diverge(env, List(thenp, elsep), NoSymbol)
          NonReducible(reason, If(cond, exprT, exprE))
        }
      }

      // e.g., while loop
      case LabelDef(name, params, rhs) => {
        val fun = DefDef(tree.symbol, List(params map { p => ValDef(p.symbol) }), rhs).copyAttrs(tree)
        val blk = Block(List(fun, Apply(Ident(fun.symbol), params)), mkUnit)
        reduce(blk, env)
      }

      case Match(selector, cases)        => throw new UnsupportedOperationException("Not implemented yet")
      case CaseDef(pat, guard, body)     => throw new UnsupportedOperationException("Not implemented yet")
      case Alternative(trees)            => throw new UnsupportedOperationException("Not implemented yet")
      case Star(elem)                    => throw new UnsupportedOperationException("Not implemented yet")
      case Bind(name, body)              => throw new UnsupportedOperationException("Not implemented yet")
      case UnApply(fun: Tree, args)      => throw new UnsupportedOperationException("Not implemented yet")

      /*
       *  Correctly computing all possible field accesses and mutations requires complete coverage of all possible 
       *  code paths. Exceptions, when combined with non-determinism (If, Match), make that difficult because they 
       *  unwind the call stack. If a Try handler is found, then all of its branches need to be re-evaluated in panic 
       *  mode - including the possibility that the exception is unhandled and propagates further up the stack, since 
       *  its type may not be statically known in panic mode. A CPS transformation would theoretically make this possible 
       *  to accomplish, but it will result in the entire program being evaluated in panic mode. Since the result of such 
       *  an evaluation is useless ("everything is read, everything is mutated, and I know nothing about the result"), 
       *  it's equally effective and far simpler to just give up if a Throw or Try is encountered.
       * 
       *  Note that Scala's Return statement is implemented by throwing scala.runtime.NonLocalReturnControl. Since 
       *  this type is not magic (user code can throw/catch it just like any other exception), Return inherits all 
       *  the same difficulties encountered when dealing with exceptions. Of course, if static analysis reveals that 
       *  the user's program does not manually catch[-NonLocalReturnControl] or throw[+NonLocalReturnControl], then 
       *  Return could be treated as a special case. Doing so would still require a CPS transformation, since return
       *  statements (which always apply to the next enclosing DefDef) can appear inside lambda expressions.
       */
      case _: Return | _: Throw | _: Try => throw new UnsupportedOperationException("Unsupported construct of type " + tree.getSimpleClassName)

      // Just in case we forgot anything...
      case _                             => throw new UnsupportedOperationException("Unknown construct of type " + tree.getSimpleClassName)
    }
  }
}