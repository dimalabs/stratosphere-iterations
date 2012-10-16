package eu.stratosphere.pact4s.compiler.util.treeReducers

import eu.stratosphere.pact4s.compiler.util._

trait Closures { this: HasGlobal with Environments =>

  import global._

  abstract class Closure extends SymTree {

    def unpacked: (Environment, List[ValDef], Tree)

    def getParams: List[List[ValDef]] = {
      val (_, params, body) = unpacked
      params :: (body match {
        case fun: Closure => fun.getParams
        case _            => Nil
      })
    }

    override def productArity = 3
    override def productElement(n: Int): Any = n match {
      case 0 => unpacked._1
      case 1 => unpacked._2
      case 2 => unpacked._3
      case _ => throw new IndexOutOfBoundsException
    }
    override def canEqual(that: Any): Boolean = that match {
      case _: Closure => true
      case _          => false
    }
  }

  object Closure {
    def unapply(fun: Closure): Some[(Environment, List[ValDef], Tree)] = Some(fun.unpacked)
  }
}