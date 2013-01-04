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

package eu.stratosphere.pact4s.common.stubs

import java.util.{ Iterator => JIterator }

import eu.stratosphere.pact4s.common.analysis.UDTSerializer

import eu.stratosphere.pact.common.`type`.PactRecord

protected class DeserializingIterator[T](deserializer: UDTSerializer[T]) extends Iterator[T] {

  private var source: JIterator[PactRecord] = null
  private var firstRecord: PactRecord = null
  private var fresh = true

  def initialize(records: JIterator[PactRecord]) = {
    source = records

    if (source.hasNext) {
      firstRecord = source.next()
      fresh = true
    } else {
      firstRecord = null
      fresh = false
    }
  }

  def hasNext = fresh || source.hasNext

  def next() = {

    if (fresh) {
      fresh = false
      deserializer.deserialize(firstRecord)
    } else {
      deserializer.deserialize(source.next())
    }
  }

  def getFirstRecord: PactRecord = firstRecord
}