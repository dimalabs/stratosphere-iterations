package eu.stratosphere.pact4s.common

import eu.stratosphere.pact4s.common.analyzer.UDT
import eu.stratosphere.pact4s.common.contracts.Pact4sContractFactory
import eu.stratosphere.pact4s.common.contracts.Pact4sContract

abstract class DataStream[T: UDT] extends Hintable[T] with Pact4sContractFactory with Serializable