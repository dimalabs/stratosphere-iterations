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

trait UDTDescriptors { this: Pact4sGlobal =>

  import global._

  abstract sealed class UDTDescriptor {
    val id: Int
    val tpe: Type

    def flatten: Seq[UDTDescriptor]

    def findById(id: Int): Option[UDTDescriptor] = flatten.find { _.id == id }

    def findByType[T <: UDTDescriptor: Manifest]: Seq[T] = {
      val clazz = implicitly[Manifest[T]].erasure
      flatten filter { item => clazz.isAssignableFrom(item.getClass) } map { _.asInstanceOf[T] }
    }
  }

  case class UnsupportedDescriptor(id: Int, tpe: Type, errors: Seq[String]) extends UDTDescriptor {
    override def flatten = Seq(this)
  }

  case class PrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapper: Symbol) extends UDTDescriptor {
    override def flatten = Seq(this)
  }

  case class BoxedPrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapper: Symbol, box: Tree => Tree, unbox: Tree => Tree) extends UDTDescriptor {

    override def flatten = Seq(this)

    override def hashCode() = (id, tpe, default, wrapper, "BoxedPrimitiveDescriptor").hashCode()
    override def equals(that: Any) = that match {
      case BoxedPrimitiveDescriptor(thatId, thatTpe, thatDefault, thatWrapper, _, _) => (id, tpe, default, wrapper).equals(thatId, thatTpe, thatDefault, thatWrapper)
      case _ => false
    }
  }

  case class ListDescriptor(id: Int, tpe: Type, listType: Type, bf: Tree, iter: Tree => Tree, elem: UDTDescriptor) extends UDTDescriptor {

    override def flatten = this +: elem.flatten

    override def hashCode() = (id, tpe, listType, elem).hashCode()
    override def equals(that: Any) = that match {
      case ListDescriptor(thatId, thatTpe, thatListType, _, _, thatElem) => (id, tpe, listType, elem).equals((thatId, thatTpe, thatListType, thatElem))
      case _ => false
    }
  }

  case class BaseClassDescriptor(id: Int, tpe: Type, getters: Seq[FieldAccessor], subTypes: Seq[UDTDescriptor]) extends UDTDescriptor {

    override def flatten = this +: ((getters flatMap { _.desc.flatten }) ++ (subTypes flatMap { _.flatten }))
  }

  case class CaseClassDescriptor(id: Int, tpe: Type, ctor: Symbol, ctorTpe: Type, getters: Seq[FieldAccessor]) extends UDTDescriptor {

    override def flatten = this +: (getters flatMap { _.desc.flatten })

    // Hack: ignore the ctorTpe, since two Type instances representing
    // the same ctor function type don't appear to be considered equal. 
    // Equality of the tpe and ctor fields implies equality of ctorTpe anyway.
    override def hashCode = (id, tpe, ctor, getters).hashCode
    override def equals(that: Any) = that match {
      case CaseClassDescriptor(thatId, thatTpe, thatCtor, _, thatGetters) => (id, tpe, ctor, getters).equals(thatId, thatTpe, thatCtor, thatGetters)
      case _ => false
    }
  }

  case class FieldAccessor(sym: Symbol, tpe: Type, isBaseField: Boolean, desc: UDTDescriptor)

  case class OpaqueDescriptor(id: Int, tpe: Type, ref: Tree) extends UDTDescriptor {

    override def flatten = Seq(this)

    // Use string representation of Trees to approximate structural hashing and
    // equality, since Tree doesn't provide an implementation of these methods.
    override def hashCode() = (id, tpe, ref.toString).hashCode()
    override def equals(that: Any) = that match {
      case OpaqueDescriptor(thatId, thatTpe, thatRef) => (id, tpe, ref.toString).equals((thatId, thatTpe, thatRef.toString))
      case _ => false
    }
  }

  case class RecursiveDescriptor(id: Int, tpe: Type, refId: Int) extends UDTDescriptor {
    override def flatten = Seq(this)
  }
}

