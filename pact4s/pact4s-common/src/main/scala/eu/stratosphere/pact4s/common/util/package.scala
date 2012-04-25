package eu.stratosphere.pact4s.common

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration

package object util {

  implicit def contract2Configurable(contract: Contract): ConfigurableContract = new WrappedContract(contract) with ConfigurableContract
  implicit def config2Configurable(config: Configuration): ConfigurableConfiguration = new WrappedConfiguration(config) with ConfigurableConfiguration
}