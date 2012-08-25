package eu.stratosphere.pact4s.compiler.util

class Counter {
  private var value: Int = 0

  def next: Int = {
    val current = value
    value += 1
    current
  }
}