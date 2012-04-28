package eu.stratosphere.pact4s.common.stubs.parameters

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration

class StubParameters extends Serializable

object StubParameters {
  private val parameterName = "Pact4s Stub Parameters"

  def getValue[T <: StubParameters](config: Configuration): T = {

    val data = config.getString(parameterName, null)

    val decoder = new sun.misc.BASE64Decoder
    val byteData = decoder.decodeBuffer(data)

    val bais = new ByteArrayInputStream(byteData)
    val ois = new ObjectInputStream(bais)

    try {
      ois.readObject().asInstanceOf[T]

    } finally {
      ois.close
      bais.close
    }
  }

  def setValue[T <: StubParameters](config: Configuration, parameters: T): Unit = {

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

  def setValue[T <: StubParameters](contract: Contract, parameters: T): Unit = setValue(contract.getParameters(), parameters)
}