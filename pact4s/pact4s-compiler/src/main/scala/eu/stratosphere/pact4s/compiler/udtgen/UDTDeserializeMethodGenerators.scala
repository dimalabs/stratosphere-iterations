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

package eu.stratosphere.pact4s.compiler.udtgen

import eu.stratosphere.pact4s.compiler.Pact4sPlugin

trait UDTDeserializeMethodGenerators { this: Pact4sPlugin with UDTSerializerClassGenerators =>

  import global._
  import defs._

  trait UDTDeserializeMethodGenerator { this: TreeGenerator with UDTSerializerClassGenerator =>

    protected def mkDeserialize(udtSerClassSym: Symbol, desc: UDTDescriptor, listImpls: Map[Int, Type]): List[Tree] = {

      val root = mkMethod(udtSerClassSym, "deserialize", Flags.OVERRIDE | Flags.FINAL, List(("record", pactRecordClass.tpe)), desc.tpe) { _ =>
        desc match {
          case PrimitiveDescriptor(_, _, default, _) => default
          case BoxedPrimitiveDescriptor(_, _, _, _, _, _) => Literal(Constant(null))
          case _ => Literal(Constant(null))
        }
      }

      List(root)
    }
  }
}