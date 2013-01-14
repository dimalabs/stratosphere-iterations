/**
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
 */

package eu.stratosphere.pact4s.compiler.util

import scala.collection.mutable

class MutableMultiMap[K, V] extends mutable.MultiMap[K, V] {

  private val inner = mutable.Map[K, mutable.Set[V]]()

  override def empty: MutableMultiMap[K, V] = new MutableMultiMap[K, V]()

  override def iterator: Iterator[(K, mutable.Set[V])] = inner.iterator

  override def contains(key: K): Boolean = inner.contains(key)

  override def get(key: K): Option[mutable.Set[V]] = Some(new mutable.Set[V]() {

    override def contains(value: V): Boolean = inner.getOrElse(key, mutable.Set[V]()).contains(value)
    override def iterator: Iterator[V] = inner.getOrElse(key, mutable.Set[V]()).iterator
    override def +=(value: V): this.type = { inner.getOrElseUpdate(key, mutable.Set[V]()).add(value); this }
    override def -=(value: V): this.type = { if (inner.contains(key)) { inner(key) -= value }; this }
  })

  override def +=(kv: (K, mutable.Set[V])): this.type = {
    if (kv._2.isEmpty)
      inner.remove(kv._1)
    else
      inner(kv._1) = kv._2
    this
  }

  override def -=(key: K): this.type = { inner.remove(key); this }
}