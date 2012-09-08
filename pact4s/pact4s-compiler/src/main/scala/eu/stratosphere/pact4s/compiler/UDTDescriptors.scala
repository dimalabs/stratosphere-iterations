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

trait UDTDescriptors { this: Pact4sPlugin =>

  import global._

  abstract sealed class UDTDescriptor {
    val id: Int
    val tpe: Type
    val isPrimitiveProduct: Boolean = false

    def mkRoot: UDTDescriptor = this

    def flatten: Seq[UDTDescriptor]
    def getters: Seq[FieldAccessor] = Seq()

    def select(member: String): Option[UDTDescriptor] = getters find { _.sym.name.toString == member } map { _.desc }

    def findById(id: Int): Option[UDTDescriptor] = flatten.find { _.id == id }

    def findByType[T <: UDTDescriptor: Manifest]: Seq[T] = {
      val clazz = implicitly[Manifest[T]].erasure
      flatten filter { item => clazz.isAssignableFrom(item.getClass) } map { _.asInstanceOf[T] }
    }

    def getRecursiveRefs: Seq[UDTDescriptor] = findByType[RecursiveDescriptor] flatMap { rd => findById(rd.refId) } map { _.mkRoot } distinct
  }

  case class UnsupportedDescriptor(id: Int, tpe: Type, errors: Seq[String]) extends UDTDescriptor {
    override def flatten = Seq(this)
  }

  case class PrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapper: Symbol) extends UDTDescriptor {
    override val isPrimitiveProduct = true
    override def flatten = Seq(this)
  }

  case class BoxedPrimitiveDescriptor(id: Int, tpe: Type, default: Literal, wrapper: Symbol, box: Tree => Tree, unbox: Tree => Tree) extends UDTDescriptor {

    override val isPrimitiveProduct = true
    override def flatten = Seq(this)

    override def hashCode() = (id, tpe, default, wrapper, "BoxedPrimitiveDescriptor").hashCode()
    override def equals(that: Any) = that match {
      case BoxedPrimitiveDescriptor(thatId, thatTpe, thatDefault, thatWrapper, _, _) => (id, tpe, default, wrapper).equals(thatId, thatTpe, thatDefault, thatWrapper)
      case _ => false
    }
  }

  case class ListDescriptor(id: Int, tpe: Type, listType: Type, cbf: () => Tree, iter: Tree => Tree, elem: UDTDescriptor) extends UDTDescriptor {

    private val cbfString = cbf().toString

    override def flatten = this +: elem.flatten

    def getInnermostElem: UDTDescriptor = elem match {
      case list: ListDescriptor => list.getInnermostElem
      case _                    => elem
    }

    override def hashCode() = (id, tpe, listType, cbfString, elem).hashCode()
    override def equals(that: Any) = that match {
      case that @ ListDescriptor(thatId, thatTpe, thatListType, _, _, thatElem) => (id, tpe, listType, cbfString, elem).equals((thatId, thatTpe, thatListType, that.cbfString, thatElem))
      case _ => false
    }
  }

  case class BaseClassDescriptor(id: Int, tpe: Type, override val getters: Seq[FieldAccessor], subTypes: Seq[UDTDescriptor]) extends UDTDescriptor {

    override def flatten = this +: ((getters flatMap { _.desc.flatten }) ++ (subTypes flatMap { _.flatten }))
  }

  case class CaseClassDescriptor(id: Int, tpe: Type, ctor: Symbol, ctorTpe: Type, override val getters: Seq[FieldAccessor]) extends UDTDescriptor {

    override val isPrimitiveProduct = !getters.isEmpty && getters.forall(_.desc.isPrimitiveProduct)

    override def mkRoot = this.copy(getters = getters map { _.copy(isBaseField = false) })
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

  case class OpaqueDescriptor(id: Int, tpe: Type, ref: () => Tree) extends UDTDescriptor {

    private val refString = ref().toString
    
    override val isPrimitiveProduct = true
    override def flatten = Seq(this)

    // Use string representation of Trees to approximate structural hashing and
    // equality, since Tree doesn't provide an implementation of these methods.
    override def hashCode() = (id, tpe, refString).hashCode()
    override def equals(that: Any) = that match {
      case that @ OpaqueDescriptor(thatId, thatTpe, _) => (id, tpe, refString).equals((thatId, thatTpe, that.refString))
      case _ => false
    }
  }

  case class RecursiveDescriptor(id: Int, tpe: Type, refId: Int) extends UDTDescriptor {
    override def flatten = Seq(this)
  }
}

