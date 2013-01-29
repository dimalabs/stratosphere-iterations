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

package eu.stratosphere.pact4s.common.stubs

import java.io._

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration

class StubParameters extends Serializable {

  def persist(config: Configuration) = StubParameters.setValue(config, this)
  def persist(contract: Contract) = StubParameters.setValue(contract, this)
}

object StubParameters {
  
  /*
   * Tried using the new config.getBytes/setBytes methods,
   * but getBytes would only return null (despite the key
   * being present and setBytes having written real data).
   * The built-in sun.misc.BASE64 encoder works...
   */
  
  private val parameterName = "pact4s.stub.parameters"

  def getValue[T <: StubParameters](config: Configuration): T = {

    val classLoader = config.getClassLoader()
    val data = config.getString(parameterName, null)

    val decoder = new sun.misc.BASE64Decoder
    val byteData = decoder.decodeBuffer(data)

    val bais = new ByteArrayInputStream(byteData)

    val ois = new ObjectInputStream(bais) {

      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        try {
          classLoader.loadClass(desc.getName());
        } catch {
          case e: ClassNotFoundException => super.resolveClass(desc)
        }
      }
    }

    try {
      ois.readObject().asInstanceOf[T]

    } finally {
      ois.close
      bais.close
    }
  }

  def setValue(config: Configuration, parameters: StubParameters): Unit = {

    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)

    try {
      oos.writeObject(parameters)

      val encoder = new sun.misc.BASE64Encoder
      val data = encoder.encode(baos.toByteArray)

      config.setString(parameterName, data)

    } finally {
      oos.close
      baos.close
    }
  }

  def setValue(contract: Contract, parameters: StubParameters): Unit = setValue(contract.getParameters(), parameters)
}

