package eu.stratosphere.pact4s.common.stubs

import java.util.{ Iterator => JIterator }

import eu.stratosphere.pact4s.common.analyzer.UDTSerializer

import eu.stratosphere.pact.common.`type`.PactRecord

protected class DeserializingIterator(deserializer: UDTSerializer[Any]) extends Iterator[Any] {

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