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

object Util {

  def partitionByType[T, S <: T: Manifest](items: List[T]): (List[T], List[S]) = {
    val sClass = implicitly[Manifest[S]].erasure
    val (sAsT, t) = items.partition { item => sClass.isAssignableFrom(item.getClass) }
    (t, sAsT map { _.asInstanceOf[S] })
  }
  
  class Counter {
    private var value: Int = 0
    
    def next: Int = {
      val current = value
      value += 1
      current
    }
  }
}
