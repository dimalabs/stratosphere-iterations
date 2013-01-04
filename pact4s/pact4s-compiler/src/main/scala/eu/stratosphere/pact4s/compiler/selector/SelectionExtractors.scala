package eu.stratosphere.pact4s.compiler.selector

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait SelectionExtractors extends SelectionValidators { this: Pact4sPlugin =>

  import global._
  import defs._

  trait SelectionExtractor extends SelectionValidator { this: TypingTransformer with TreeGenerator with Logger =>

    protected object FieldSelection {

      def unapply(tree: Tree): Option[(Type, Type, Tree, Either[List[String], (Tree, List[List[String]])])] = tree match {

        case Apply(TypeApply(view, List(t1, r)), List(fun)) if view.symbol == unanalyzedFieldSelector => {

          val ret = getSelector(fun) match {
            case Left(errs) => Left(errs)
            case Right(sels) => getUDT(t1.tpe) match {
              case Left(err) => Left(List(err))
              case Right((udtTree, udtDesc)) => chkSelectors(udtDesc, sels) match {
                case Nil  => Right(udtTree, sels map { _.tail })
                case errs => Left(errs)
              }
            }
          }

          Some(t1.tpe, r.tpe, fun, ret)
        }

        case _ => None
      }
    }

    protected object KeySelection {

      def unapply(tree: Tree): Option[(Type, Type, Tree, Either[List[String], (Tree, List[List[String]])])] = tree match {
        
        case Apply(TypeApply(view, List(t1, r)), List(fun)) if view.symbol == unanalyzedKeySelector => {
          
          val ret = getSelector(fun) match {
            case Left(errs) => Left(errs)
            case Right(sels) => getUDT(t1.tpe) match {
              case Left(err) => Left(List(err))
              case Right((udtTree, udtDesc)) => chkSelectors(udtDesc, sels) match {
                case Nil  => Right(udtTree, sels map { _.tail })
                case errs => Left(errs)
              }
            }
          }

          Some(t1.tpe, r.tpe, fun, ret)
        }
        
        case _ => None
      }
    }

    private def getSelector(tree: Tree): Either[List[String], List[List[String]]] = tree match {

      case Function(List(p), body) => getSelector(body, Map(p.symbol -> Nil)) match {
        case err @ Left(_) => err
        case Right(sels)   => Right(sels map { sel => p.name.toString +: sel })
      }

      case _ => Left(List("expected lambda expression literal but found " + tree.getSimpleClassName))
    }

    private def getSelector(tree: Tree, roots: Map[Symbol, List[String]]): Either[List[String], List[List[String]]] = tree match {

      case SimpleMatch(body, bindings)                => getSelector(body, roots ++ bindings)

      case Match(_, List(CaseDef(pat, EmptyTree, _))) => Left(List("case pattern is too complex"))
      case Match(_, List(CaseDef(_, guard, _)))       => Left(List("case pattern is guarded"))
      case Match(_, _ :: _ :: _)                      => Left(List("match contains more than one case"))

      case TupleCtor(args) => {

        val (errs, sels) = args.map(arg => getSelector(arg, roots)).partition(_.isLeft)

        errs match {
          case Nil => Right(sels.map(_.right.get).flatten)
          case _   => Left(errs.map(_.left.get).flatten)
        }
      }

      case Apply(Select(New(tpt), _), _) => Left(List("constructor call on non-tuple type " + tpt.tpe))

      case Ident(name) => roots.get(tree.symbol) match {
        case Some(sel) => Right(List(sel))
        case None      => Left(List("unexpected identifier " + name))
      }

      case Select(src, member) => getSelector(src, roots) match {
        case err @ Left(_)    => err
        case Right(List(sel)) => Right(List(sel :+ member.toString))
        case _                => Left(List("unsupported selection"))
      }

      case _ => Left(List("unsupported construct of kind " + tree.getSimpleClassName))

    }

    private object SimpleMatch {

      def unapply(tree: Tree): Option[(Tree, Map[Symbol, List[String]])] = tree match {

        case Match(arg, List(cd @ CaseDef(CasePattern(bindings), EmptyTree, body))) => Some((body, bindings))
        case _ => None
      }

      private object CasePattern {

        def unapply(tree: Tree): Option[Map[Symbol, List[String]]] = tree match {

          case Apply(MethodTypeTree(params), binds) => {

            val exprs = params.zip(binds) map {
              case (p, CasePattern(inners)) => Some(inners map { case (sym, path) => (sym, p.name.toString +: path) })
              case _                        => None
            }

            if (exprs.forall(_.isDefined))
              Some(exprs.flatten.flatten.toMap)
            else
              None
          }

          case Ident(_) | Bind(_, Ident(_)) => Some(Map(tree.symbol -> Nil))
          case Bind(_, CasePattern(inners)) => Some(inners + (tree.symbol -> Nil))
          case _                            => None
        }
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

    private object TupleCtor {

      def unapply(tree: Tree): Option[List[Tree]] = tree match {
        case Apply(Select(New(tpt), _), args) if isTupleTpe(tpt.tpe) => Some(args)
        case _                                                       => None
      }

      private def isTupleTpe(tpe: Type): Boolean = definitions.TupleClass.contains(tpe.typeSymbol)
    }
  }
}
