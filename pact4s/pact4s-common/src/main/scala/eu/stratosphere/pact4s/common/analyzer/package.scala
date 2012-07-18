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

package eu.stratosphere.pact4s.common

package object analyzer {

  type UDF1Builder[T1, R] = { type UDF[_] = UDF1[T1 => R] }
  type UDF2Builder[T1, T2, R] = { type UDF[_] = UDF2[(T1, T2) => R] }
  type SelectorBuilder[T1, R] = { type Selector[_] = FieldSelector[T1 => R] }

  def defaultUDF1[T1: UDT, R: UDT]: UDF1[T1 => R] = new AnalyzedUDF1[T1, R](implicitly[UDT[T1]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF1IterR[T1: UDT, R: UDT]: UDF1[T1 => Iterator[R]] = new AnalyzedUDF1[T1, Iterator[R]](implicitly[UDT[T1]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF1IterT[T1: UDT, R: UDT]: UDF1[Iterator[T1] => R] = new AnalyzedUDF1[Iterator[T1], R](implicitly[UDT[T1]].numFields, implicitly[UDT[R]].numFields)

  def defaultUDF2[T1: UDT, T2: UDT, R: UDT]: UDF2[(T1, T2) => R] = new AnalyzedUDF2[T1, T2, R](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF2IterR[T1: UDT, T2: UDT, R: UDT]: UDF2[(T1, T2) => Iterator[R]] = new AnalyzedUDF2[T1, T2, Iterator[R]](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF2IterT[T1: UDT, T2: UDT, R: UDT]: UDF2[(Iterator[T1], Iterator[T2]) => R] = new AnalyzedUDF2[Iterator[T1], Iterator[T2], R](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)
  def defaultUDF2IterTR[T1: UDT, T2: UDT, R: UDT]: UDF2[(Iterator[T1], Iterator[T2]) => Iterator[R]] = new AnalyzedUDF2[Iterator[T1], Iterator[T2], Iterator[R]](implicitly[UDT[T1]].numFields, implicitly[UDT[T2]].numFields, implicitly[UDT[R]].numFields)

  def defaultFieldSelectorT[T1: UDT, R]: FieldSelector[T1 => R] = new AnalyzedFieldSelector[T1, R](implicitly[UDT[T1]].numFields)
  def defaultFieldSelectorR[T1, R: UDT]: FieldSelector[T1 => R] = new AnalyzedFieldSelector[T1, R](implicitly[UDT[R]].numFields)

  def getFieldSelector[T1: UDT, R](selFields: Int*): FieldSelector[T1 => R] = new AnalyzedFieldSelector[T1, R](implicitly[UDT[T1]].numFields) {
    for (field <- 0 until implicitly[UDT[T1]].numFields if !selFields.contains(field))
      markFieldUnused(field)
  }
}

