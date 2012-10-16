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