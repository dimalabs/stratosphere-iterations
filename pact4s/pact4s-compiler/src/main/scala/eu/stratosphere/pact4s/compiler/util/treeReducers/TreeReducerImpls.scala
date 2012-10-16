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

trait TreeReducerImpls { this: HasGlobal with TreeReducers with TreeGenerators with Loggers =>

  import global._
  import Extractors._
  import TreeReducerHelpers._

  private object Extractors {

    object ObjectDef {
      def unapply(tree: Tree) = tree match {
        case ClassDef(mods, name, tpts, impl) if tree.symbol.isModuleClass => Some((mods, name, tpts, impl))
        case _                                                             => None
      }
    }

    object LazyAccessorDef {
      def unapply(tree: Tree): Option[(Modifiers, TermName, List[TypeDef], List[List[ValDef]], Tree, Tree)] = tree match {
        case DefDef(mods, name, tparams, paramss, tpt, rhs) if tree.hasSymbolWhich(sym => sym.isLazyAccessor) => Some((mods, name, tparams, paramss, tpt, rhs))
        case _ => None
      }
    }

    object CasePattern {

      def unapply(tree: Tree): Option[(List[(Symbol, Seq[Symbol])])] = tree match {

        case Apply(MethodTypeTree(params), binds) => {

          val exprs = params.zip(binds) map {
            case (p, CasePattern(inners)) => {
              val sel = p.owner.owner.info.decl(p.name) filter (_.isParamAccessor)
              val binds = inners flatMap {
                case (NoSymbol, _) => None
                case (sym, path)   => Some((sym, sel +: path))
              }
              Some(binds)
            }
            case _ => None
          }

          if (exprs.forall(_.isDefined))
            Some(exprs.flatten.flatten)
          else
            None
        }

        case Ident(_)                     => Some(List((tree.symbol, Seq())))
        case Bind(_, Ident(_))            => Some(List((tree.symbol, Seq())))
        case Bind(_, CasePattern(inners)) => Some((tree.symbol, Seq()) +: inners)
        case _                            => None
      }

      private object MethodTypeTree {
        def unapply(tree: Tree): Option[List[Symbol]] = tree match {
          case _: TypeTree => tree.tpe match {
            case MethodType(params, _) => Some(params)
            case _                     => None
          }
          case _ => None
        }
      }
    }

  }

  trait TreeReducerImpl extends InheritsGlobal { this: TreeGenerator with Logger with TreeReducer with EnvironmentBuilders =>

    class DeterministicTreeReducer(val strictBlocks: Boolean, val origTree: Tree) {

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
          val pkg = env.get(tree.symbol) match {
            case None => {
              val pkg = env.makeChild setSymbol tree.symbol
              env(tree.symbol) = pkg
              pkg
            }
            case Some(pkg: Environment) => pkg
          }

          stats foreach { reduce(_, pkg) }
          mkUnit
        }

        case ObjectDef(mods, name, tpts, impl @ Template(ps, self, body)) => {

          val classEnv = {
            // Evaluate this tree as a normal class definition
            try {
              tree.symbol.resetFlag(Flags.MODULE)
              val anonEnv = env.makeChild
              reduce(tree, anonEnv)
              anonEnv(tree.symbol)
            } finally {
              tree.symbol.setFlag(Flags.MODULE)
            }
          }

          // Get the primary constructor body
          val ctorSym = tree.symbol.primaryConstructor
          val body = classEnv match {
            case classEnv: Environment => classEnv(ctorSym) match {
              case Closure(_, Nil, body) => body
              case other                 => throw new UnsupportedOperationException("Invalid object definition @ " + tree.pos)
            }
            case other => throw new UnsupportedOperationException("Invalid object definition @ " + tree.pos)
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

          env.initMember(classSym, mkClosure(env, classSym.newValue("<objInit>"), Nil, rhs))
          mkUnit
        }

        case ClassDef(mods, name, tpts, impl @ Template(parents, self, body)) => {

          val classSym = tree.symbol
          val classEnv = env.makeChild setSymbol classSym
          env(classSym) = classEnv

          val (ctors, cstats) = body filter { t =>
            t match {
              case _: ValDef | _: DefDef => !t.symbol.isParamAccessor
              case _                     => true
            }
          } partition {
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

            case _ => throw new UnsupportedOperationException("Invalid constructor definition @ " + tree.pos)
          }

          mkUnit
        }

        case LazyAccessorDef(mods, name, tparams, paramss, tpt, rhs) => {

          val Block(List(Assign(_, init)), _) = rhs

          // Make a function which evaluates the expression and then overwrites itself with the result.
          val sel = Select(This(tree.symbol.owner), tree.symbol)
          val upd = Assign(sel, init)

          env.initMember(tree.symbol, mkClosure(env, tree.symbol, Nil, Block(List(upd), sel)))
          mkUnit
        }

        case ValDef(_, _, _, rhs)             => env(tree.symbol) = reduce(rhs, env); mkUnit
        case DefDef(_, _, _, paramss, _, rhs) => env(tree.symbol) = mkClosure({ env.makeChild }, tree.symbol, paramss, rhs); mkUnit

        // Ctor call site
        case Select(New(from: TypeTree), _)   => reduce(Select(New(Ident(from.symbol)), tree.symbol), env)

        case Select(New(from), _) => reduce(Select(from, tree.symbol), env) match {

          case ctor @ Closure(classEnv, params, body) => {
            val inst = classEnv.makeChild setSymbol SymbolFactory.makeInstanceOf(classEnv.symbol)
            TypeOf(inst) = classEnv.symbol
            val ctorBody = new Block(List(body), inst) { override val hasSymbol = true }
            mkClosure(inst, ctor.symbol, List(params), ctorBody)
          }

          // Unknown ctor - pack the parameters into a panicked instance environment 
          case ctor: NonReducible => {

            val classSym = tree.symbol.owner
            val inst = Environment.Empty.makeChild setSymbol SymbolFactory.makeInstanceOf(classSym)
            val paramss = tree.symbol.paramss map { params => params map { ValDef(_) } }

            for (sym <- classSym.tpe.members)
              inst(sym) = NonReducible(ReductionError.NoSource, EmptyTree)

            // panic the instance after the ctor parameters have been applied
            val body = Apply(mkClosure(inst, tree.symbol.newValue(nme.CONSTRUCTOR), Nil, NonReducible(ReductionError.CausedBy(ctor), tree)), Nil)
            val ctorBody = new Block(List(body), inst) { override val hasSymbol = true }

            mkClosure(inst, tree.symbol, paramss, ctorBody)
          }

          case _ => throw new UnsupportedOperationException("Invalid constructor call @ " + tree.pos)
        }

        // Ctor call site within an auxilliary ctor
        case Select(This(_), _) if tree.symbol.isConstructor => env(tree.symbol) match {

          // When stepping from one ctor to another, keep the same instance environment
          case ctor @ Closure(_, params, body) => mkClosure(env, ctor.symbol, List(params), body)

          case _                               => throw new UnsupportedOperationException("Invalid self-constructor call @ " + tree.pos)
        }

        // Super ctor call site within a subclass
        case Select(Super(_, _), _) if tree.symbol.isConstructor => env.find(tree.symbol) match {

          case Some(ctor @ Closure(superClassEnv, params, body)) => {

            CtorFlag.checkAndSet(env, superClassEnv.symbol) match {

              // This super class's ctor has already been called, so this call is a no-op
              case false => mkClosure(env.makeChild, ctor.symbol, ctor.getParams, mkUnit)

              case true => {

                // Make a proxy to the instance environment with the super class as 
                // its parent, so that the super class's outer classes are accessible.
                val inst = env.asChildOf(superClassEnv) setSymbol SymbolFactory.makeInstanceOf(superClassEnv.symbol)
                mkClosure(inst, ctor.symbol, List(params), body)
              }
            }
          }

          // Unknown super class
          case _ => {

            // definitions.AnyCompanionClass = scala.AnyCompanion doesn't exist
            val tops = Set(definitions.AnyClass, definitions.AnyValClass, definitions.AnyValCompanionClass, definitions.AnyRefClass, definitions.ObjectClass)
            val topCtors = tops flatMap { _.tpe.members.filter(_.isConstructor) }

            val paramss = tree.symbol.paramss map { params => params map { ValDef(_) } }

            for (sym <- tree.symbol.owner.tpe.members)
              env(sym) = NonReducible(ReductionError.NoSource, EmptyTree)

            topCtors.contains(tree.symbol) match {
              case true  => mkClosure(env, tree.symbol, paramss, mkUnit)
              case false => mkClosure(env, tree.symbol, paramss, NonReducible(ReductionError.NoSource, tree))
            }
          }
        }

        // The program has already been type checked, so just unwrap type applications
        case Typed(expr, tpt)                                      => reduce(expr, env)
        case TypeApply(fun, args)                                  => reduce(fun, env)

        // Calls to parameterless methods (and call-by-name arg refs) don't get Apply nodes, so reduce them on access
        case Closure(env, Nil, body) if !tree.symbol.isConstructor => reduce(body, env)

        // Types, literals, instances, unapplied closures, and non-reducible statements are already in WHNF
        case _: TypeDef | _: TypTree | _: Literal | EmptyTree      => tree
        case _: Environment | _: Closure | _: NonReducible         => tree
        case ArrayValue(tpt, elems)                                => treeCopy.ArrayValue(tree, tpt, elems map { reduce(_, env) })

        case CurrentEnvironment                                    => env

        case Function(vparams, body)                               => mkClosure({ env.makeChild }, tree.symbol, List(vparams), body)

        case Ident(_) if tree.symbol.isModule || tree.symbol.isClass => env.find(tree.symbol) match {
          case Some(ref) => ref
          case None      => NonReducible(ReductionError.NoSource, tree)
        }

        case Ident(_) => env.findParent(_.defines(tree.symbol)) match {
          case Some(owner) => reduce(owner(tree.symbol), env)
          case _ => {
            val keys = env.toMap.keys.map(_.name).mkString(", ")
            throw new UnsupportedOperationException("Invalid local reference: " + tree.symbol.name + " not in " + keys + " @ " + tree.pos)
          }
        }

        case This(_) if tree.symbol.isPackageClass => env.find(tree.symbol) match {
          case Some(pkg) => pkg
          case None      => NonReducible(ReductionError.NoSource, tree)
        }

        case This(_) => env.findParent(_.symbol eq SymbolFactory.makeInstanceOf(tree.symbol)) match {
          case Some(inst) => inst
          case None       => throw new UnsupportedOperationException("Invalid this reference @ " + tree.pos)
        }

        // Super call site within a subclass - ignore overriding
        case Select(Super(_, _), _) => env.get(tree.symbol) match {
          case Some(value) => reduce(value, env)
          case None        => NonReducible(ReductionError.NoSource, tree)
        }

        // Normal selection - respect overriding
        case Select(from, member) => reduce(from, env) match {

          case inst: Environment => {

            val sym = tree.symbol.overridingSymbol(TypeOf(inst)) match {
              case null | NoSymbol => tree.symbol
              case sym             => sym
            }

            inst.get(sym) match {
              case Some(value) => reduce(value, env)
              case None => {

                inst.toMap.keys.find { k => k.name.toString == sym.name.toString } match {
                  // this shouldn't happen
                  case Some(k) => throw new UnsupportedOperationException("Invalid selection " + tree + ": " + sym.name + " was found but has unexpected symbol @ " + tree.pos)

                  // this can happen
                  case None    => Warn.report("Selection failed: " + tree + " @ " + tree.pos)
                }

                NonReducible(ReductionError.NoSource, tree)
              }
            }
          }

          case inst: Literal                               => NonReducible(ReductionError.NoSource, Select(inst, tree.symbol))
          case inst: Closure if member.toString == "apply" => inst
          case inst: Closure                               => NonReducible(ReductionError.NoSource, Select(inst, tree.symbol))
          case inst: NonReducible                          => NonReducible(ReductionError.CausedBy(inst), Select(inst.expr, tree.symbol))
          case inst                                        => throw new UnsupportedOperationException("Invalid selection " + tree + ": source has type " + inst.getSimpleClassName + " @ " + tree.pos)
        }

        case Apply(fun, args) => reduce(fun, env) match {

          case rFun @ Closure(evalEnv, vparams, body) => {

            val params = vparams map {
              case vd if vd.symbol.owner.isConstructor => vd.symbol.owner.owner.info.decl(vd.symbol.name) filter (_.isParamAccessor)
              case vd                                  => vd.symbol
            }

            body match {

              case body: NonReducible => {

                val rArgs = args map { reduce(_, env) }

                for ((sym, arg) <- params.zip(args))
                  evalEnv(sym) = arg

                NonReducible.panicked(ReductionError.CausedBy(body), mkClosure(evalEnv, rFun.symbol, Nil, body))
              }

              case _ => {

                val rArgs = params.zip(args) map {
                  case (sym, arg) if sym.isByNameParam => (sym, mkClosure(env, sym, Nil, arg)) // call-by-name: pass arg unevaluated
                  case (sym, arg)                      => (sym, reduce(arg, env))
                }

                for ((sym, arg) <- rArgs)
                  evalEnv(sym) = arg

                callStack.enter(rFun.symbol) {

                  reduce(body, evalEnv)

                } getOrElse {

                  diverge(evalEnv, List(body), rFun.symbol)
                  NonReducible(ReductionError.Recursive, Apply(rFun, rArgs.map(_._2)))
                }
              }
            }
          }

          case rFun: NonReducible => NonReducible.panicked(ReductionError.CausedBy(rFun), Apply(rFun.expr, args map { reduce(_, env) }).copyAttrs(tree))

          case _                  => throw new UnsupportedOperationException("Invalid function application @ " + tree.pos)
        }

        case Assign(lhs, rhs) => {

          val target = lhs match {

            case Ident(_) => env.findParent(_.defines(lhs.symbol)) match {
              case Some(target) => target
              case _            => throw new UnsupportedOperationException("Invalid assignment @" + tree.pos)
            }

            case Select(from, _) => reduce(from, env)
          }

          target match {
            // target(lhs.symbol) is always a var, so no need to worry about overriding
            case target: Environment  => target(lhs.symbol) = reduce(rhs, env); mkUnit

            case target: NonReducible => NonReducible(ReductionError.CausedBy(target), Assign(target.expr, reduce(rhs, env)))
            case _                    => throw new UnsupportedOperationException("Invalid assignment @ " + tree.pos)
          }
        }

        case Block(stats, expr) => {

          val blockEnv = tree.hasSymbol match {
            case true  => env // body of a ctor function
            case false => env.makeChild // anonymous block or body of non-ctor function
          }

          val inits = stats flatMap {
            case stat: ValDef => Some(stat.symbol)
            case _            => None
          }

          val orderedStats = stats.zipWithIndex sortBy {
            case (_: ValDef, index)    => (1, index)
            case (_: MemberDef, index) => (0, index)
            case (_, index)            => (1, index)
          } map { _._1 }

          for (sym <- inits) {
            blockEnv.initMember(sym, mkDefault(sym.tpe.typeSymbol))
          }

          val rStats = orderedStats map { reduce(_, blockEnv) }
          val rExpr = reduce(expr, blockEnv)

          val notReduced = rStats find {
            case _: NonReducible => true
            case _               => false
          }

          (strictBlocks && !tree.hasSymbol, notReduced) match {
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

        case Match(arg, cases) => {

          val expr = reduce(arg, env) match {

            case rArg: NonReducible => NonReducible(ReductionError.CausedBy(rArg), rArg)

            case rArg => cases match {

              case List(cd @ CaseDef(CasePattern(bindings), EmptyTree, body)) => {
                val (syms, sels) = bindings.unzip
                val funParams = syms map {
                  case NoSymbol => throw new UnsupportedOperationException("Pattern variable is missing symbol")
                  case sym      => ValDef(sym) setSymbol sym setPos tree.pos
                }
                val funArgs = sels map { path => mkSelectSyms(rArg, path: _*) setPos tree.pos }
                val fun = Function(funParams, body) setSymbol SymbolFactory.makeNonRecursiveAnonFun

                Apply(fun, funArgs)
              }

              case List(CaseDef(pat, EmptyTree, _)) => NonReducible(ReductionError.TooComplex, pat)
              case _                                => NonReducible(ReductionError.NonDeterministic, tree)
            }
          }

          expr match {

            case _: NonReducible => {

              val branches = cases flatMap {
                case CaseDef(_, EmptyTree, body) => Seq(body)
                case CaseDef(_, guard, body)     => Seq(guard, body)
              }

              diverge(env, branches, SymbolFactory.makeNonRecursiveAnonFun)
              expr
            }

            case _ => reduce(expr, env)
          }
        }

        /*
        case Match(selector, cases)        => NonReducible(ReductionError.NotImplemented, tree)
        case CaseDef(pat, guard, body)     => NonReducible(ReductionError.NotImplemented, tree)
        
        case Bind(name, body)              => NonReducible(ReductionError.NotImplemented, tree)

        case Alternative(trees)            => NonReducible(ReductionError.NotImplemented, tree)
        case Star(elem)                    => NonReducible(ReductionError.NotImplemented, tree)
        */

        case UnApply(fun: Tree, args)      => NonReducible(ReductionError.NotImplemented, tree)

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
        case _: Return | _: Throw | _: Try => throw new UnsupportedOperationException("Unsupported construct of type " + tree.getSimpleClassName + " @ " + tree.pos)

        // Just in case we forgot anything...
        case _                             => throw new UnsupportedOperationException("Unknown construct of type " + tree.getSimpleClassName + " @ " + tree.pos)
      }
    }
  }
}