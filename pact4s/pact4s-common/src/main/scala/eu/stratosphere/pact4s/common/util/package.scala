package eu.stratosphere.pact4s.common

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration

package object util {

  type ForEachAble[A] = { def foreach[U](f: A => U): Unit }

}