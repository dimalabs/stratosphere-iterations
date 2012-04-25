package eu.stratosphere.pact4s.common

package object streams {
  type ForEachAble[A] = { def foreach[U](f: A => U): Unit }
}